"""
Data Quality Validation Script

Run this after ingestion to validate data quality:
- Check for missing data
- Verify row counts
- Spot-check known tickers and events
- Identify outliers and anomalies
"""

from pathlib import Path

import pandas as pd


def validate_membership(data_root: Path) -> dict:
    """Validate S&P 500 membership data."""
    print("\n" + "=" * 60)
    print("Validating S&P 500 Membership")
    print("=" * 60)
    
    results = {}
    membership_file = data_root / "equities/index_membership/sp500.parquet"
    
    if not membership_file.exists():
        print("✗ Membership file not found")
        results["status"] = "missing"
        return results
    
    df = pd.read_parquet(membership_file)
    
    results["total_rows"] = len(df)
    results["unique_tickers"] = df["ticker"].nunique()
    results["current_members"] = df["end_date"].isna().sum()
    results["delisted_members"] = df["end_date"].notna().sum()
    
    print(f"  Total rows: {results['total_rows']}")
    print(f"  Unique tickers: {results['unique_tickers']}")
    print(f"  Current members: {results['current_members']}")
    print(f"  Delisted members: {results['delisted_members']}")
    
    # Check for required columns
    required_cols = ["ticker", "index_name", "start_date"]
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        print(f"  ✗ Missing columns: {missing_cols}")
        results["status"] = "invalid"
    else:
        print("  ✓ All required columns present")
        results["status"] = "ok"
    
    # Sector breakdown
    if "sector" in df.columns:
        print("\n  Top sectors:")
        for sector, count in df["sector"].value_counts().head(5).items():
            print(f"    {sector}: {count}")
    
    return results


def validate_ohlcv(data_root: Path) -> dict:
    """Validate OHLCV data."""
    print("\n" + "=" * 60)
    print("Validating OHLCV Data")
    print("=" * 60)
    
    results = {}
    ohlcv_root = data_root / "equities/ohlcv/daily/us"
    
    if not ohlcv_root.exists():
        print("✗ OHLCV directory not found")
        results["status"] = "missing"
        return results
    
    # Find all partition files
    parquet_files = list(ohlcv_root.rglob("*.parquet"))
    
    if not parquet_files:
        print("✗ No OHLCV files found")
        results["status"] = "missing"
        return results
    
    print(f"  Found {len(parquet_files)} partition files")
    
    # Read all data (could be slow for large datasets)
    print("  Reading data...")
    all_data = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        all_data.append(df)
    
    combined = pd.concat(all_data, ignore_index=True)
    
    results["total_rows"] = len(combined)
    results["unique_tickers"] = combined["ticker"].nunique()
    results["date_range"] = (
        str(combined["date"].min()),
        str(combined["date"].max()),
    )
    results["exchanges"] = combined["exchange"].unique().tolist()
    
    print(f"  Total rows: {results['total_rows']:,}")
    print(f"  Unique tickers: {results['unique_tickers']}")
    print(f"  Date range: {results['date_range'][0]} to {results['date_range'][1]}")
    print(f"  Exchanges: {', '.join(results['exchanges'])}")
    
    # Check for missing values
    missing = combined[["open", "high", "low", "close", "volume"]].isna().sum()
    print("\n  Missing values:")
    for col, count in missing.items():
        pct = (count / len(combined)) * 100
        print(f"    {col}: {count} ({pct:.2f}%)")
    
    results["missing_values"] = missing.to_dict()
    
    # Check for anomalies (negative prices, zero volume)
    anomalies = 0
    if (combined["open"] <= 0).any():
        anomalies += (combined["open"] <= 0).sum()
    if (combined["high"] <= 0).any():
        anomalies += (combined["high"] <= 0).sum()
    if (combined["low"] <= 0).any():
        anomalies += (combined["low"] <= 0).sum()
    if (combined["close"] <= 0).any():
        anomalies += (combined["close"] <= 0).sum()
    
    if anomalies > 0:
        print(f"\n  ⚠ Found {anomalies} rows with non-positive prices")
        results["anomalies"] = anomalies
    else:
        print("\n  ✓ No anomalies detected")
    
    # Spot check: AAPL
    if "AAPL.US" in combined["ticker"].values:
        aapl = combined[combined["ticker"] == "AAPL.US"].sort_values("date")
        print("\n  Spot check: AAPL.US")
        print(f"    Rows: {len(aapl)}")
        print(f"    Date range: {aapl['date'].min()} to {aapl['date'].max()}")
        print(f"    Latest close: ${aapl['close'].iloc[-1]:.2f}")
    
    results["status"] = "ok"
    return results


def validate_corporate_actions(data_root: Path) -> dict:
    """Validate corporate actions (dividends and splits)."""
    print("\n" + "=" * 60)
    print("Validating Corporate Actions")
    print("=" * 60)
    
    results = {}
    
    # Dividends
    dividends_file = data_root / "equities/corporate-actions/dividends.parquet"
    if dividends_file.exists():
        dividends = pd.read_parquet(dividends_file)
        results["dividends_rows"] = len(dividends)
        results["dividends_tickers"] = dividends["ticker"].nunique()
        print(f"  Dividends: {results['dividends_rows']:,} rows, {results['dividends_tickers']} tickers")
    else:
        print("  ✗ Dividends file not found")
        results["dividends_rows"] = 0
    
    # Splits
    splits_file = data_root / "equities/corporate-actions/splits.parquet"
    if splits_file.exists():
        splits = pd.read_parquet(splits_file)
        results["splits_rows"] = len(splits)
        results["splits_tickers"] = splits["ticker"].nunique()
        print(f"  Splits: {results['splits_rows']:,} rows, {results['splits_tickers']} tickers")
        
        # Spot check: AAPL 4:1 split on 2020-08-31
        aapl_splits = splits[
            (splits["ticker"] == "AAPL.US") &
            (splits["date"] >= "2020-08-01") &
            (splits["date"] <= "2020-09-30")
        ]
        if not aapl_splits.empty:
            print("\n  Spot check: AAPL 4:1 split (2020-08-31)")
            print(f"    Found: {aapl_splits[['date', 'split_ratio', 'split_factor']].to_dict('records')}")
        else:
            print("\n  ⚠ AAPL 4:1 split not found (expected 2020-08-31)")
    else:
        print("  ✗ Splits file not found")
        results["splits_rows"] = 0
    
    results["status"] = "ok"
    return results


def validate_fundamentals(data_root: Path) -> dict:
    """Validate fundamentals data."""
    print("\n" + "=" * 60)
    print("Validating Fundamentals")
    print("=" * 60)
    
    results = {}
    
    # Annual statements
    annual_file = data_root / "equities/fundamentals/annual/statements.parquet"
    if annual_file.exists():
        annual = pd.read_parquet(annual_file)
        results["annual_rows"] = len(annual)
        results["annual_tickers"] = annual["ticker"].nunique()
        print(f"  Annual statements: {results['annual_rows']:,} rows, {results['annual_tickers']} tickers")
    else:
        print("  ✗ Annual statements not found")
        results["annual_rows"] = 0
    
    # Quarterly statements
    quarterly_file = data_root / "equities/fundamentals/quarterly/statements.parquet"
    if quarterly_file.exists():
        quarterly = pd.read_parquet(quarterly_file)
        results["quarterly_rows"] = len(quarterly)
        results["quarterly_tickers"] = quarterly["ticker"].nunique()
        print(f"  Quarterly statements: {results['quarterly_rows']:,} rows, {results['quarterly_tickers']} tickers")
    else:
        print("  ✗ Quarterly statements not found")
        results["quarterly_rows"] = 0
    
    # Ratios
    ratios_file = data_root / "equities/fundamentals/ratios/ratios.parquet"
    if ratios_file.exists():
        ratios = pd.read_parquet(ratios_file)
        results["ratios_rows"] = len(ratios)
        results["ratios_tickers"] = ratios["ticker"].nunique()
        print(f"  Ratios: {results['ratios_rows']:,} rows, {results['ratios_tickers']} tickers")
    else:
        print("  ✗ Ratios not found")
        results["ratios_rows"] = 0
    
    # Classifications
    class_file = data_root / "equities/fundamentals/classifications/classifications.parquet"
    if class_file.exists():
        classifications = pd.read_parquet(class_file)
        results["classifications_rows"] = len(classifications)
        print(f"  Classifications: {results['classifications_rows']:,} rows")
    else:
        print("  ✗ Classifications not found")
        results["classifications_rows"] = 0
    
    results["status"] = "ok"
    return results


def validate_ingestion_log(data_root: Path) -> dict:
    """Validate ingestion log."""
    print("\n" + "=" * 60)
    print("Validating Ingestion Log")
    print("=" * 60)
    
    results = {}
    log_file = data_root / "_meta/ingestion-log.parquet"
    
    if not log_file.exists():
        print("✗ Ingestion log not found")
        results["status"] = "missing"
        return results
    
    log = pd.read_parquet(log_file)
    
    results["total_runs"] = len(log)
    results["successful"] = (log["status"] == "success").sum()
    results["failed"] = (log["status"] == "failed").sum()
    results["last_run"] = str(log["timestamp"].max())
    
    print(f"  Total runs: {results['total_runs']}")
    print(f"  Successful: {results['successful']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Last run: {results['last_run']}")
    
    # Group by dataset and status
    print("\n  Runs by dataset:")
    for (dataset, status), count in log.groupby(["dataset", "status"]).size().items():
        print(f"    {dataset} ({status}): {count}")
    
    # Show recent failures
    failed = log[log["status"] == "failed"].sort_values("timestamp", ascending=False).head(5)
    if not failed.empty:
        print("\n  Recent failures:")
        for _, row in failed.iterrows():
            print(f"    {row['dataset']}/{row['ticker']}: {row['error_message']}")
    
    results["status"] = "ok"
    return results


def main():
    """Run all validation checks."""
    # Get data root from config
    config_path = Path(__file__).parent.parent / "config" / "ingest.yaml"
    
    if config_path.exists():
        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f)
        data_root = Path(config["data_root"])
    else:
        data_root = Path(__file__).parent.parent
    
    print("\n" + "=" * 60)
    print("Data Quality Validation")
    print("=" * 60)
    print(f"Data root: {data_root}")
    
    # Run all validations
    results = {
        "membership": validate_membership(data_root),
        "ohlcv": validate_ohlcv(data_root),
        "corporate_actions": validate_corporate_actions(data_root),
        "fundamentals": validate_fundamentals(data_root),
        "ingestion_log": validate_ingestion_log(data_root),
    }
    
    # Summary
    print("\n" + "=" * 60)
    print("Validation Summary")
    print("=" * 60)
    
    all_ok = True
    for dataset, result in results.items():
        status = result.get("status", "unknown")
        if status == "ok":
            print(f"  ✓ {dataset}: OK")
        elif status == "missing":
            print(f"  ✗ {dataset}: Missing data")
            all_ok = False
        else:
            print(f"  ⚠ {dataset}: {status}")
    
    if all_ok:
        print("\n✓ All validations passed!")
    else:
        print("\n⚠ Some validations failed. Review output above.")
    
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
