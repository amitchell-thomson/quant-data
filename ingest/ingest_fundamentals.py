"""
Fundamentals Data Ingestion

Downloads fundamental data for S&P 500 constituents:
- Annual and quarterly financial statements (income, balance, cash flow)
- Key ratios and metrics
- Shares outstanding and market cap
- Company classifications (sector, industry, etc.)
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from tqdm import tqdm

from .common.eodhd_client import EODHDClient
from .common.io_parquet import read_parquet, upsert_parquet
from .common.logging import IngestionLogger, print_ingestion_summary
from .common.schema import (
    CLASSIFICATIONS_SCHEMA,
    CLASSIFICATIONS_PRIMARY_KEY,
    CLASSIFICATIONS_REQUIRED_COLS,
    FUNDAMENTALS_SCHEMA,
    FUNDAMENTALS_PRIMARY_KEY,
    FUNDAMENTALS_REQUIRED_COLS,
    RATIOS_SCHEMA,
    RATIOS_PRIMARY_KEY,
    RATIOS_REQUIRED_COLS,
    SHARES_OUTSTANDING_SCHEMA,
    SHARES_OUTSTANDING_PRIMARY_KEY,
    SHARES_OUTSTANDING_REQUIRED_COLS,
)


def parse_financial_statements(
    data: dict,
    ticker: str,
    period_type: str,
) -> pd.DataFrame:
    """
    Parse financial statements (annual or quarterly).
    
    Args:
        data: Fundamentals data from EODHD
        ticker: Ticker symbol
        period_type: "annual" or "quarterly"
        
    Returns:
        DataFrame with financial statement data
    """
    financials = data.get("Financials", {})
    
    if period_type == "annual":
        statements = financials.get("Balance_Sheet", {}).get("yearly", {})
        income = financials.get("Income_Statement", {}).get("yearly", {})
        cashflow = financials.get("Cash_Flow", {}).get("yearly", {})
    else:  # quarterly
        statements = financials.get("Balance_Sheet", {}).get("quarterly", {})
        income = financials.get("Income_Statement", {}).get("quarterly", {})
        cashflow = financials.get("Cash_Flow", {}).get("quarterly", {})
    
    if not statements and not income and not cashflow:
        return pd.DataFrame()
    
    # Combine all dates from all statements
    all_dates = set()
    all_dates.update(statements.keys())
    all_dates.update(income.keys())
    all_dates.update(cashflow.keys())
    
    records = []
    for date_str in all_dates:
        # Parse date and fiscal info
        try:
            period_end = pd.to_datetime(date_str).date()
            fiscal_year = period_end.year
            fiscal_quarter = ((period_end.month - 1) // 3) + 1 if period_type == "quarterly" else None
        except:
            continue
        
        # Extract fields from each statement
        bs = statements.get(date_str, {})
        inc = income.get(date_str, {})
        cf = cashflow.get(date_str, {})
        
        record = {
            "ticker": ticker,
            "period_end_date": period_end,
            "report_date": None,  # EODHD doesn't always provide filing date
            "period_type": period_type,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "currency": data.get("General", {}).get("CurrencyCode"),
            
            # Income Statement
            "revenue": inc.get("totalRevenue"),
            "cost_of_revenue": inc.get("costOfRevenue"),
            "gross_profit": inc.get("grossProfit"),
            "operating_expense": inc.get("operatingExpenses"),
            "operating_income": inc.get("operatingIncome"),
            "ebitda": inc.get("ebitda"),
            "net_income": inc.get("netIncome"),
            "eps_basic": inc.get("basicEPS"),
            "eps_diluted": inc.get("dilutedEPS"),
            
            # Balance Sheet
            "total_assets": bs.get("totalAssets"),
            "total_liabilities": bs.get("totalLiab"),
            "total_equity": bs.get("totalStockholderEquity"),
            "cash_and_equivalents": bs.get("cash"),
            "short_term_debt": bs.get("shortTermDebt"),
            "long_term_debt": bs.get("longTermDebt"),
            
            # Cash Flow
            "operating_cash_flow": cf.get("totalCashFromOperatingActivities"),
            "investing_cash_flow": cf.get("totalCashflowsFromInvestingActivities"),
            "financing_cash_flow": cf.get("totalCashFromFinancingActivities"),
            "free_cash_flow": cf.get("freeCashFlow"),
            "capex": cf.get("capitalExpenditures"),
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Convert numeric columns
    numeric_cols = [
        "revenue", "cost_of_revenue", "gross_profit", "operating_expense",
        "operating_income", "ebitda", "net_income", "eps_basic", "eps_diluted",
        "total_assets", "total_liabilities", "total_equity", "cash_and_equivalents",
        "short_term_debt", "long_term_debt", "operating_cash_flow",
        "investing_cash_flow", "financing_cash_flow", "free_cash_flow", "capex",
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return df


def parse_ratios(data: dict, ticker: str) -> pd.DataFrame:
    """
    Parse key ratios and metrics.
    
    Args:
        data: Fundamentals data from EODHD
        ticker: Ticker symbol
        
    Returns:
        DataFrame with ratios
    """
    highlights = data.get("Highlights", {})
    valuation = data.get("Valuation", {})
    technicals = data.get("Technicals", {})
    
    # Get current date for as-of
    as_of_date = datetime.now().date()
    
    record = {
        "ticker": ticker,
        "date": as_of_date,
        "period_type": "ttm",  # Most ratios are TTM
        
        # Valuation
        "market_cap": highlights.get("MarketCapitalization"),
        "enterprise_value": highlights.get("EnterpriseValue"),
        "pe_ratio": highlights.get("PERatio"),
        "pb_ratio": highlights.get("PriceBookMRQ"),
        "ps_ratio": highlights.get("PriceSalesTTM"),
        "peg_ratio": highlights.get("PEGRatio"),
        "ev_to_ebitda": highlights.get("EnterpriseValueEbitda"),
        "ev_to_sales": highlights.get("EnterpriseValueRevenue"),
        
        # Profitability
        "gross_margin": highlights.get("GrossProfitTTM"),
        "operating_margin": highlights.get("OperatingMarginTTM"),
        "profit_margin": highlights.get("ProfitMargin"),
        "roa": highlights.get("ReturnOnAssetsTTM"),
        "roe": highlights.get("ReturnOnEquityTTM"),
        "roic": valuation.get("Roic"),
        
        # Liquidity & Solvency
        "current_ratio": highlights.get("CurrentRatio"),
        "quick_ratio": highlights.get("QuickRatio"),
        "debt_to_equity": highlights.get("DebtToEquity"),
        "debt_to_assets": None,  # Compute if needed
        
        # Efficiency
        "asset_turnover": highlights.get("AssetTurnover"),
        "inventory_turnover": highlights.get("InventoryTurnover"),
        "receivables_turnover": highlights.get("ReceivablesTurnover"),
    }
    
    df = pd.DataFrame([record])
    
    # Convert numeric columns
    numeric_cols = [col for col in df.columns if col not in ["ticker", "date", "period_type"]]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return df


def parse_shares_outstanding(data: dict, ticker: str) -> pd.DataFrame:
    """
    Parse shares outstanding and market cap.
    
    Args:
        data: Fundamentals data from EODHD
        ticker: Ticker symbol
        
    Returns:
        DataFrame with shares outstanding
    """
    shares_stats = data.get("SharesStats", {})
    highlights = data.get("Highlights", {})
    
    as_of_date = datetime.now().date()
    
    record = {
        "ticker": ticker,
        "date": as_of_date,
        "shares_outstanding": shares_stats.get("SharesOutstanding"),
        "shares_outstanding_diluted": shares_stats.get("SharesOutstandingDiluted"),
        "float_shares": shares_stats.get("SharesFloat"),
        "market_cap": highlights.get("MarketCapitalization"),
    }
    
    df = pd.DataFrame([record])
    
    # Convert numeric columns
    numeric_cols = ["shares_outstanding", "shares_outstanding_diluted", "float_shares", "market_cap"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return df


def parse_classifications(data: dict, ticker: str) -> pd.DataFrame:
    """
    Parse company classifications (sector, industry, etc.).
    
    Args:
        data: Fundamentals data from EODHD
        ticker: Ticker symbol
        
    Returns:
        DataFrame with classification data
    """
    general = data.get("General", {})
    
    record = {
        "ticker": ticker,
        "company_name": general.get("Name"),
        "exchange": general.get("Exchange"),
        "sector": general.get("Sector"),
        "industry": general.get("Industry"),
        "gics_sector": general.get("GicSector"),
        "gics_industry": general.get("GicIndustry"),
        "country": general.get("CountryISO"),
        "currency": general.get("CurrencyCode"),
        "isin": general.get("ISIN"),
        "cusip": general.get("CUSIP"),
        "last_updated": datetime.now().date(),
    }
    
    df = pd.DataFrame([record])
    return df


def fetch_fundamentals_for_ticker(
    client: EODHDClient,
    ticker: str,
    include_annual: bool,
    include_quarterly: bool,
    include_ratios: bool,
    include_shares: bool,
    include_classifications: bool,
) -> dict:
    """
    Fetch fundamental data for a single ticker.
    
    Args:
        client: EODHD client
        ticker: Ticker symbol
        include_annual: Include annual statements
        include_quarterly: Include quarterly statements
        include_ratios: Include ratios
        include_shares: Include shares outstanding
        include_classifications: Include classifications
        
    Returns:
        Dict with results for each data type
    """
    try:
        # Fetch full fundamentals data
        data = client.get_fundamentals(ticker)
        
        result = {
            "ticker": ticker,
            "status": "success",
        }
        
        # Parse different components
        if include_annual:
            annual_df = parse_financial_statements(data, ticker, "annual")
            result["annual"] = annual_df
            result["annual_rows"] = len(annual_df)
        
        if include_quarterly:
            quarterly_df = parse_financial_statements(data, ticker, "quarterly")
            result["quarterly"] = quarterly_df
            result["quarterly_rows"] = len(quarterly_df)
        
        if include_ratios:
            ratios_df = parse_ratios(data, ticker)
            result["ratios"] = ratios_df
            result["ratios_rows"] = len(ratios_df)
        
        if include_shares:
            shares_df = parse_shares_outstanding(data, ticker)
            result["shares"] = shares_df
            result["shares_rows"] = len(shares_df)
        
        if include_classifications:
            classifications_df = parse_classifications(data, ticker)
            result["classifications"] = classifications_df
            result["classifications_rows"] = len(classifications_df)
        
        return result
    
    except Exception as e:
        return {
            "ticker": ticker,
            "status": "failed",
            "error_message": str(e),
        }


def ingest_fundamentals(
    config_path: Path | str,
    api_key: str,
    tickers: Optional[list[str]] = None,
    max_workers: int = 3,  # Lower for fundamentals (heavier requests)
    dry_run: bool = False,
    force: bool = False,
    annual: bool = True,
    quarterly: bool = True,
    ratios: bool = True,
    shares: bool = True,
    classifications: bool = True,
) -> None:
    """
    Ingest fundamental data.
    
    Args:
        config_path: Path to ingest.yaml config file
        api_key: EODHD API key
        tickers: Optional list of specific tickers
        max_workers: Number of parallel workers
        dry_run: If True, don't write data
        force: If True, re-download even if data exists
        annual: Include annual statements
        quarterly: Include quarterly statements
        ratios: Include ratios/metrics
        shares: Include shares outstanding
        classifications: Include company classifications
    """
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    data_root = Path(config["data_root"])
    membership_file = data_root / config["universe"]["sp500"]["membership_file"]
    annual_path = data_root / config["datasets"]["fundamentals"]["annual_path"]
    quarterly_path = data_root / config["datasets"]["fundamentals"]["quarterly_path"]
    ratios_path = data_root / config["datasets"]["fundamentals"]["ratios_path"]
    shares_path = data_root / config["datasets"]["fundamentals"]["shares_outstanding_path"]
    classifications_path = data_root / config["datasets"]["fundamentals"]["classifications_path"]
    log_path = data_root / config["logging"]["ingestion_log_path"]
    
    print(f"\n{'=' * 60}")
    print("Fundamentals Data Ingestion")
    print(f"{'=' * 60}")
    print(f"Annual: {annual}")
    print(f"Quarterly: {quarterly}")
    print(f"Ratios: {ratios}")
    print(f"Shares: {shares}")
    print(f"Classifications: {classifications}")
    print(f"Workers: {max_workers}")
    print(f"Dry run: {dry_run}")
    print(f"{'=' * 60}\n")
    
    # Load membership to get ticker list
    if tickers is None:
        print("Loading S&P 500 membership...")
        membership_df = read_parquet(membership_file)
        
        if membership_df.empty:
            print("Error: No membership data found. Run membership ingestion first.")
            sys.exit(1)
        
        tickers = membership_df["ticker"].unique().tolist()
        print(f"Found {len(tickers)} unique tickers in membership")
    
    # Initialize logger
    logger = IngestionLogger(log_path)
    
    # Create client
    client = EODHDClient(
        api_key=api_key,
        base_url=config["api"]["base_url"],
        timeout=config["api"]["timeout"],
        max_retries=config["api"]["max_retries"],
        retry_backoff_factor=config["api"]["retry_backoff_factor"],
        rate_limit_rps=config["api"]["rate_limit"]["requests_per_second"],
        rate_limit_burst=config["api"]["rate_limit"]["burst_size"],
    )
    
    # Fetch data in parallel
    print(f"\nFetching fundamentals for {len(tickers)} tickers...")
    
    results = []
    successful = 0
    failed = 0
    
    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_fundamentals_for_ticker,
                client,
                ticker,
                annual,
                quarterly,
                ratios,
                shares,
                classifications,
            ): ticker
            for ticker in tickers
        }
        
        with tqdm(total=len(tickers), desc="Downloading", unit="ticker") as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if result["status"] == "success":
                    successful += 1
                else:
                    failed += 1
                
                pbar.update(1)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Write data
    if not dry_run and results:
        print("\nWriting data...")
        
        # Annual statements
        if annual:
            annual_data = [r.get("annual") for r in results if "annual" in r and not r["annual"].empty]
            if annual_data:
                combined_annual = pd.concat(annual_data, ignore_index=True)
                print(f"  Annual statements: {len(combined_annual):,} rows")
                
                annual_file = annual_path / "statements.parquet"
                rows_new, rows_updated = upsert_parquet(
                    combined_annual,
                    annual_file,
                    FUNDAMENTALS_SCHEMA,
                    FUNDAMENTALS_REQUIRED_COLS,
                    FUNDAMENTALS_PRIMARY_KEY,
                    "fundamentals_annual",
                )
                print(f"    New: {rows_new}, Updated: {rows_updated}")
        
        # Quarterly statements
        if quarterly:
            quarterly_data = [r.get("quarterly") for r in results if "quarterly" in r and not r["quarterly"].empty]
            if quarterly_data:
                combined_quarterly = pd.concat(quarterly_data, ignore_index=True)
                print(f"  Quarterly statements: {len(combined_quarterly):,} rows")
                
                quarterly_file = quarterly_path / "statements.parquet"
                rows_new, rows_updated = upsert_parquet(
                    combined_quarterly,
                    quarterly_file,
                    FUNDAMENTALS_SCHEMA,
                    FUNDAMENTALS_REQUIRED_COLS,
                    FUNDAMENTALS_PRIMARY_KEY,
                    "fundamentals_quarterly",
                )
                print(f"    New: {rows_new}, Updated: {rows_updated}")
        
        # Ratios
        if ratios:
            ratios_data = [r.get("ratios") for r in results if "ratios" in r and not r["ratios"].empty]
            if ratios_data:
                combined_ratios = pd.concat(ratios_data, ignore_index=True)
                print(f"  Ratios: {len(combined_ratios):,} rows")
                
                ratios_file = ratios_path / "ratios.parquet"
                rows_new, rows_updated = upsert_parquet(
                    combined_ratios,
                    ratios_file,
                    RATIOS_SCHEMA,
                    RATIOS_REQUIRED_COLS,
                    RATIOS_PRIMARY_KEY,
                    "ratios",
                )
                print(f"    New: {rows_new}, Updated: {rows_updated}")
        
        # Shares outstanding
        if shares:
            shares_data = [r.get("shares") for r in results if "shares" in r and not r["shares"].empty]
            if shares_data:
                combined_shares = pd.concat(shares_data, ignore_index=True)
                print(f"  Shares outstanding: {len(combined_shares):,} rows")
                
                shares_file = shares_path / "shares.parquet"
                rows_new, rows_updated = upsert_parquet(
                    combined_shares,
                    shares_file,
                    SHARES_OUTSTANDING_SCHEMA,
                    SHARES_OUTSTANDING_REQUIRED_COLS,
                    SHARES_OUTSTANDING_PRIMARY_KEY,
                    "shares_outstanding",
                )
                print(f"    New: {rows_new}, Updated: {rows_updated}")
        
        # Classifications
        if classifications:
            class_data = [r.get("classifications") for r in results if "classifications" in r and not r["classifications"].empty]
            if class_data:
                combined_class = pd.concat(class_data, ignore_index=True)
                print(f"  Classifications: {len(combined_class):,} rows")
                
                class_file = classifications_path / "classifications.parquet"
                rows_new, rows_updated = upsert_parquet(
                    combined_class,
                    class_file,
                    CLASSIFICATIONS_SCHEMA,
                    CLASSIFICATIONS_REQUIRED_COLS,
                    CLASSIFICATIONS_PRIMARY_KEY,
                    "classifications",
                )
                print(f"    New: {rows_new}, Updated: {rows_updated}")
        
        # Log results
        log_results = [
            {
                "ticker": r["ticker"],
                "status": r["status"],
                "rows_ingested": sum([
                    r.get("annual_rows", 0),
                    r.get("quarterly_rows", 0),
                    r.get("ratios_rows", 0),
                    r.get("shares_rows", 0),
                    r.get("classifications_rows", 0),
                ]),
                "error_message": r.get("error_message"),
            }
            for r in results
        ]
        logger.log_batch("fundamentals", log_results)
    
    # Print summary
    total_rows = sum([
        sum([r.get("annual_rows", 0) for r in results]),
        sum([r.get("quarterly_rows", 0) for r in results]),
        sum([r.get("ratios_rows", 0) for r in results]),
        sum([r.get("shares_rows", 0) for r in results]),
        sum([r.get("classifications_rows", 0) for r in results]),
    ])
    
    print_ingestion_summary(
        dataset="fundamentals",
        total_tickers=len(tickers),
        successful=successful,
        failed=failed,
        total_rows=total_rows,
        duration_seconds=duration,
    )
    
    print("\n✓ Fundamentals ingestion complete")
    
    client.close()


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        print("Error: EODHD_API_KEY not found in environment")
        sys.exit(1)
    
    config_path = Path(__file__).parent.parent / "config" / "ingest.yaml"
    
    # Parse CLI args
    import argparse
    parser = argparse.ArgumentParser(description="Ingest fundamental data")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    parser.add_argument("--max-workers", type=int, default=3, help="Parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    parser.add_argument("--annual", action="store_true", help="Only annual statements")
    parser.add_argument("--quarterly", action="store_true", help="Only quarterly statements")
    parser.add_argument("--ratios", action="store_true", help="Only ratios")
    parser.add_argument("--shares", action="store_true", help="Only shares outstanding")
    parser.add_argument("--classifications", action="store_true", help="Only classifications")
    
    args = parser.parse_args()
    
    # If no specific flags, include all
    if not any([args.annual, args.quarterly, args.ratios, args.shares, args.classifications]):
        include_all = True
    else:
        include_all = False
    
    ingest_fundamentals(
        config_path=config_path,
        api_key=api_key,
        tickers=args.tickers,
        max_workers=args.max_workers,
        dry_run=args.dry_run,
        force=args.force,
        annual=args.annual or include_all,
        quarterly=args.quarterly or include_all,
        ratios=args.ratios or include_all,
        shares=args.shares or include_all,
        classifications=args.classifications or include_all,
    )
