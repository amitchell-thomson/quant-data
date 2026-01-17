"""
Example usage of the ingested data for analysis.

This script demonstrates how to load and use the ingested data
for common quantitative research tasks.
"""

from pathlib import Path

import pandas as pd


def load_sp500_membership(data_root: Path) -> pd.DataFrame:
    """Load S&P 500 membership data."""
    return pd.read_parquet(data_root / "equities/index_membership/sp500.parquet")


def load_ohlcv_for_ticker(data_root: Path, ticker: str, exchange: str) -> pd.DataFrame:
    """
    Load OHLCV data for a specific ticker.
    
    Args:
        data_root: Path to data root
        ticker: Ticker symbol (e.g., "AAPL.US")
        exchange: Exchange (NASDAQ, NYSE, or AMEX)
    
    Returns:
        DataFrame with OHLCV data for the ticker
    """
    ohlcv_file = data_root / f"equities/ohlcv/daily/us/{exchange}/{exchange}.parquet"
    
    if not ohlcv_file.exists():
        raise FileNotFoundError(f"OHLCV file not found: {ohlcv_file}")
    
    df = pd.read_parquet(ohlcv_file)
    return df[df["ticker"] == ticker].sort_values("date")


def load_all_ohlcv(data_root: Path, tickers: list[str] = None) -> pd.DataFrame:
    """
    Load OHLCV data for all or specific tickers.
    
    Args:
        data_root: Path to data root
        tickers: Optional list of tickers to filter
    
    Returns:
        Combined DataFrame with OHLCV data
    """
    ohlcv_root = data_root / "equities/ohlcv/daily/us"
    parquet_files = list(ohlcv_root.rglob("*.parquet"))
    
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        if tickers:
            df = df[df["ticker"].isin(tickers)]
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True).sort_values(["ticker", "date"])


def load_dividends(data_root: Path, ticker: str = None) -> pd.DataFrame:
    """Load dividend data, optionally filtered by ticker."""
    df = pd.read_parquet(data_root / "equities/corporate-actions/dividends.parquet")
    
    if ticker:
        df = df[df["ticker"] == ticker]
    
    return df.sort_values(["ticker", "ex_date"])


def load_splits(data_root: Path, ticker: str = None) -> pd.DataFrame:
    """Load split data, optionally filtered by ticker."""
    df = pd.read_parquet(data_root / "equities/corporate-actions/splits.parquet")
    
    if ticker:
        df = df[df["ticker"] == ticker]
    
    return df.sort_values(["ticker", "date"])


def load_annual_fundamentals(data_root: Path, ticker: str = None) -> pd.DataFrame:
    """Load annual financial statements."""
    df = pd.read_parquet(data_root / "equities/fundamentals/annual/statements.parquet")
    
    if ticker:
        df = df[df["ticker"] == ticker]
    
    return df.sort_values(["ticker", "period_end_date"])


def load_quarterly_fundamentals(data_root: Path, ticker: str = None) -> pd.DataFrame:
    """Load quarterly financial statements."""
    df = pd.read_parquet(data_root / "equities/fundamentals/quarterly/statements.parquet")
    
    if ticker:
        df = df[df["ticker"] == ticker]
    
    return df.sort_values(["ticker", "period_end_date"])


def load_ratios(data_root: Path, ticker: str = None) -> pd.DataFrame:
    """Load valuation ratios and metrics."""
    df = pd.read_parquet(data_root / "equities/fundamentals/ratios/ratios.parquet")
    
    if ticker:
        df = df[df["ticker"] == ticker]
    
    return df.sort_values(["ticker", "date"])


# =============================================================================
# Example Analysis Functions
# =============================================================================

def calculate_returns(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily returns for each ticker."""
    ohlcv = ohlcv.sort_values(["ticker", "date"])
    ohlcv["daily_return"] = ohlcv.groupby("ticker")["close"].pct_change()
    return ohlcv


def get_dividend_yield(
    data_root: Path,
    ticker: str,
    as_of_date: str,
) -> float:
    """
    Calculate trailing 12-month dividend yield.
    
    Args:
        data_root: Path to data root
        ticker: Ticker symbol
        as_of_date: Date to calculate yield as of (YYYY-MM-DD)
    
    Returns:
        Dividend yield as decimal (e.g., 0.025 = 2.5%)
    """
    as_of_date = pd.to_datetime(as_of_date)
    one_year_ago = as_of_date - pd.Timedelta(days=365)
    
    # Get dividends in trailing 12 months
    dividends = load_dividends(data_root, ticker)
    dividends["ex_date"] = pd.to_datetime(dividends["ex_date"])
    ttm_divs = dividends[
        (dividends["ex_date"] > one_year_ago) &
        (dividends["ex_date"] <= as_of_date)
    ]
    
    total_divs = ttm_divs["amount"].sum()
    
    # Get price as of date
    # (In real usage, you'd want the exact date or closest business day)
    ohlcv = load_all_ohlcv(data_root, [ticker])
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    price_row = ohlcv[ohlcv["date"] <= as_of_date].sort_values("date").iloc[-1]
    price = price_row["close"]
    
    return total_divs / price


def get_pe_ratio_history(data_root: Path, ticker: str) -> pd.DataFrame:
    """
    Get P/E ratio over time from ratios data.
    
    Returns:
        DataFrame with date and pe_ratio columns
    """
    ratios = load_ratios(data_root, ticker)
    return ratios[["date", "pe_ratio"]].sort_values("date")


def screen_by_fundamentals(
    data_root: Path,
    min_revenue: float = 1e9,  # $1B
    min_roe: float = 0.15,     # 15%
    max_pe: float = 25,
) -> pd.DataFrame:
    """
    Screen stocks by fundamental criteria.
    
    Args:
        data_root: Path to data root
        min_revenue: Minimum annual revenue
        min_roe: Minimum ROE
        max_pe: Maximum P/E ratio
    
    Returns:
        DataFrame of tickers meeting criteria
    """
    # Load latest annual fundamentals
    annual = load_annual_fundamentals(data_root)
    latest_annual = annual.sort_values(["ticker", "period_end_date"]).groupby("ticker").tail(1)
    
    # Load latest ratios
    ratios = load_ratios(data_root)
    latest_ratios = ratios.sort_values(["ticker", "date"]).groupby("ticker").tail(1)
    
    # Merge
    merged = latest_annual.merge(
        latest_ratios[["ticker", "pe_ratio", "roe"]],
        on="ticker",
        how="inner",
    )
    
    # Apply filters
    filtered = merged[
        (merged["revenue"] >= min_revenue) &
        (merged["roe"] >= min_roe) &
        (merged["pe_ratio"] <= max_pe)
    ]
    
    return filtered[["ticker", "revenue", "net_income", "roe", "pe_ratio"]].sort_values("roe", ascending=False)


# =============================================================================
# Example Usage
# =============================================================================

def main():
    """Run example analysis."""
    # Set data root
    data_root = Path(__file__).parent.parent
    
    print("=" * 60)
    print("Example Data Analysis")
    print("=" * 60)
    
    # Example 1: Load membership
    print("\n[1] S&P 500 Membership")
    membership = load_sp500_membership(data_root)
    current_members = membership[membership["end_date"].isna()]
    print(f"  Current members: {len(current_members)}")
    print(f"  Top 5 sectors:")
    print(current_members["sector"].value_counts().head())
    
    # Example 2: Load OHLCV for AAPL
    print("\n[2] AAPL OHLCV Data")
    try:
        aapl = load_ohlcv_for_ticker(data_root, "AAPL.US", "NASDAQ")
        print(f"  Rows: {len(aapl)}")
        print(f"  Date range: {aapl['date'].min()} to {aapl['date'].max()}")
        print(f"  Latest close: ${aapl['close'].iloc[-1]:.2f}")
    except FileNotFoundError as e:
        print(f"  Error: {e}")
    
    # Example 3: Calculate dividend yield
    print("\n[3] AAPL Dividend Yield (as of 2024-12-31)")
    try:
        yield_pct = get_dividend_yield(data_root, "AAPL.US", "2024-12-31") * 100
        print(f"  Yield: {yield_pct:.2f}%")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Example 4: Screen by fundamentals
    print("\n[4] Fundamental Screen")
    try:
        screened = screen_by_fundamentals(
            data_root,
            min_revenue=10e9,  # $10B
            min_roe=0.20,      # 20%
            max_pe=20,
        )
        print(f"  Stocks meeting criteria: {len(screened)}")
        if not screened.empty:
            print("\n  Top 5:")
            print(screened.head().to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
