"""
EODHD API Client

Handles all communication with the EODHD API with:
- Automatic retries with exponential backoff
- Rate limiting to respect API quotas
- Request timeouts
- Comprehensive error handling
"""

import time
from datetime import datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util.retry import Retry


class RateLimiter:
    """Token bucket rate limiter for API requests."""
    
    def __init__(self, requests_per_second: float = 10, burst_size: int = 20):
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_update = time.time()
    
    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(
                self.burst_size,
                self.tokens + elapsed * self.requests_per_second
            )
            self.last_update = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                return
            
            # Wait until next token is available
            time.sleep((1 - self.tokens) / self.requests_per_second)


class EODHDClient:
    """Client for EODHD API with resilience features."""
    
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://eodhd.com/api",
        timeout: int = 30,
        max_retries: int = 5,
        retry_backoff_factor: float = 2.0,
        rate_limit_rps: float = 10,
        rate_limit_burst: int = 20,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        
        # Rate limiter
        self.rate_limiter = RateLimiter(rate_limit_rps, rate_limit_burst)
        
        # Session with connection pooling and retry strategy
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()
        
        # Configure retries at the urllib3 level (for connection errors)
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
            allowed_methods=["GET"],  # Only retry safe methods
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Make an API request with rate limiting and error handling.
        
        Args:
            endpoint: API endpoint (e.g., "/eod/AAPL.US")
            params: Query parameters
            
        Returns:
            JSON response as dict
            
        Raises:
            requests.RequestException: On HTTP errors
        """
        # Rate limit
        self.rate_limiter.acquire()
        
        # Add API key to params
        if params is None:
            params = {}
        params["api_token"] = self.api_key
        params["fmt"] = "json"
        
        url = f"{self.base_url}{endpoint}"
        
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        
        return response.json()
    
    # =========================================================================
    # EOD / OHLCV Data
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_eod_data(
        self,
        ticker: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get end-of-day OHLCV data for a ticker.
        
        Args:
            ticker: Ticker symbol (e.g., "AAPL.US")
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            List of daily OHLCV records
        """
        endpoint = f"/eod/{ticker}"
        params = {}
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        
        data = self._make_request(endpoint, params)
        
        # Handle both list and single-object responses
        if isinstance(data, dict):
            return [data] if data else []
        return data
    
    # =========================================================================
    # Dividends & Splits
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_dividends(
        self,
        ticker: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get dividend history for a ticker.
        
        Args:
            ticker: Ticker symbol (e.g., "AAPL.US")
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            List of dividend records
        """
        endpoint = f"/div/{ticker}"
        params = {}
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, dict):
            return [data] if data else []
        return data
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_splits(
        self,
        ticker: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get stock split history for a ticker.
        
        Args:
            ticker: Ticker symbol (e.g., "AAPL.US")
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            List of split records
        """
        endpoint = f"/splits/{ticker}"
        params = {}
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, dict):
            return [data] if data else []
        return data
    
    # =========================================================================
    # Fundamentals
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_fundamentals(
        self,
        ticker: str,
        filter_: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get fundamental data for a ticker.
        
        Args:
            ticker: Ticker symbol (e.g., "AAPL.US")
            filter_: Optional filter (e.g., "Financials::Balance_Sheet::quarterly")
            
        Returns:
            Fundamentals data as nested dict
        """
        endpoint = f"/fundamentals/{ticker}"
        params = {}
        
        if filter_:
            params["filter"] = filter_
        
        return self._make_request(endpoint, params)
    
    # =========================================================================
    # Index Constituents / Membership
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_index_constituents(
        self,
        index_code: str,
    ) -> dict[str, Any]:
        """
        Get current constituents of an index.
        
        Args:
            index_code: Index code (e.g., "GSPC.INDX" for S&P 500)
            
        Returns:
            Index data including constituents
        """
        endpoint = f"/fundamentals/{index_code}"
        return self._make_request(endpoint)
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_historical_index_constituents(
        self,
        index_code: str,
        date: str,
    ) -> dict[str, Any]:
        """
        Get historical constituents of an index on a specific date.
        
        Args:
            index_code: Index code (e.g., "GSPC.INDX" for S&P 500)
            date: Date in YYYY-MM-DD format
            
        Returns:
            Index data including constituents as of that date
            
        Note:
            EODHD's historical constituents API may have limitations.
            If unavailable, fall back to manually tracking changes.
        """
        endpoint = f"/fundamentals/{index_code}"
        params = {"historical": date}
        return self._make_request(endpoint, params)
    
    # =========================================================================
    # Macro / Economic Data
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_macro_indicator(
        self,
        indicator_code: str,
        country: str = "USA",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get macroeconomic indicator data.
        
        Args:
            indicator_code: Indicator code (e.g., "GDP", "CPI")
            country: Country code (default "USA")
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            List of indicator data points
        """
        endpoint = f"/macro-indicator/{country}/{indicator_code}"
        params = {}
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, dict):
            return [data] if data else []
        return data
    
    # =========================================================================
    # Exchange & Ticker Search
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_exchange_symbols(
        self,
        exchange: str,
        delisted: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Get all symbols trading on an exchange.
        
        Args:
            exchange: Exchange code (e.g., "US", "NYSE", "NASDAQ")
            delisted: Include delisted securities
            
        Returns:
            List of symbol information
        """
        endpoint = f"/exchange-symbol-list/{exchange}"
        params = {}
        
        if delisted:
            params["delisted"] = "1"
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, dict):
            return [data] if data else []
        return data
    
    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
