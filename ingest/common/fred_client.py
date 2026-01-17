"""
FRED (Federal Reserve Economic Data) API Client

Handles communication with the St. Louis Fed's FRED API with:
- Automatic retries with exponential backoff
- Rate limiting (120 requests per minute)
- Request timeouts
- Comprehensive error handling
"""

import time
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
    
    def __init__(self, requests_per_second: float = 2.0, burst_size: int = 10):
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


class FREDClient:
    """Client for FRED API with resilience features."""
    
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.stlouisfed.org/fred",
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff_factor: float = 2.0,
        rate_limit_rpm: float = 120,  # Requests per minute
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        
        # Rate limiter (convert requests per minute to per second)
        self.rate_limiter = RateLimiter(
            requests_per_second=rate_limit_rpm / 60,
            burst_size=10
        )
        
        # Session with connection pooling and retry strategy
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()
        
        # Configure retries at the urllib3 level (for connection errors)
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
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
            endpoint: API endpoint path (e.g., "series/observations")
            params: Query parameters
            
        Returns:
            JSON response as dict
            
        Raises:
            requests.RequestException: On HTTP errors
            ValueError: On API errors
        """
        # Rate limit
        self.rate_limiter.acquire()
        
        # Add API key to params
        if params is None:
            params = {}
        params["api_key"] = self.api_key
        params["file_type"] = "json"
        
        url = f"{self.base_url}/{endpoint}"
        
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        
        data = response.json()
        
        # Check for API-level errors
        if "error_code" in data:
            error_msg = data.get("error_message", "Unknown FRED API error")
            raise ValueError(f"FRED API error: {error_msg}")
        
        return data
    
    # =========================================================================
    # Series Data
    # =========================================================================
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_series_observations(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        frequency: Optional[str] = None,
        aggregation_method: Optional[str] = None,
        units: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get observations for a FRED series.
        
        Args:
            series_id: FRED series ID (e.g., "DGS10", "FEDFUNDS", "GDP")
            observation_start: Start date (YYYY-MM-DD)
            observation_end: End date (YYYY-MM-DD)
            frequency: Optional frequency (d=daily, w=weekly, m=monthly, q=quarterly, a=annual)
            aggregation_method: How to aggregate (avg, sum, eop=end of period)
            units: Units transformation (lin=levels, chg=change, ch1=change from year ago, pch=percent change, pc1=percent change from year ago, pca=compounded annual rate of change, cch=continuously compounded rate of change, cca=continuously compounded annual rate of change, log=natural log)
            
        Returns:
            List of observations with date, value, and metadata
            
        Example response:
            [
                {
                    "realtime_start": "2024-01-01",
                    "realtime_end": "2024-01-01",
                    "date": "2020-01-01",
                    "value": "1.55"
                },
                ...
            ]
        """
        endpoint = "series/observations"
        params = {"series_id": series_id}
        
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        if frequency:
            params["frequency"] = frequency
        if aggregation_method:
            params["aggregation_method"] = aggregation_method
        if units:
            params["units"] = units
        
        data = self._make_request(endpoint, params)
        return data.get("observations", [])
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def get_series_info(
        self,
        series_id: str,
    ) -> dict[str, Any]:
        """
        Get metadata for a FRED series.
        
        Args:
            series_id: FRED series ID
            
        Returns:
            Series metadata including title, units, frequency, etc.
            
        Example response:
            {
                "id": "DGS10",
                "realtime_start": "2024-01-01",
                "realtime_end": "2024-01-01",
                "title": "10-Year Treasury Constant Maturity Rate",
                "observation_start": "1962-01-02",
                "observation_end": "2024-01-01",
                "frequency": "Daily",
                "frequency_short": "D",
                "units": "Percent",
                "units_short": "Percent",
                "seasonal_adjustment": "Not Seasonally Adjusted",
                "seasonal_adjustment_short": "NSA",
                "last_updated": "2024-01-02 15:18:02-06",
                "popularity": 90,
                "notes": "..."
            }
        """
        endpoint = "series"
        params = {"series_id": series_id}
        
        data = self._make_request(endpoint, params)
        series_list = data.get("seriess", [])
        
        if not series_list:
            raise ValueError(f"Series {series_id} not found")
        
        return series_list[0]
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        reraise=True,
    )
    def search_series(
        self,
        search_text: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Search for FRED series by text.
        
        Args:
            search_text: Search query
            limit: Maximum number of results
            
        Returns:
            List of matching series
        """
        endpoint = "series/search"
        params = {
            "search_text": search_text,
            "limit": limit,
        }
        
        data = self._make_request(endpoint, params)
        return data.get("seriess", [])
    
    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
