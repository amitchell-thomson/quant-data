# Quick Start Guide

Get up and running with the ingestion pipeline in 5 minutes.

## Setup (One-time)

```bash
# 1. Install dependencies
pip install -e .

# 2. Set up your API key
echo "EODHD_API_KEY=your_actual_api_key_here" > .env

# 3. Verify configuration
cat config/ingest.yaml
```

## Basic Usage

### Option 1: Run Everything (Recommended for first time)

```bash
# Full pipeline with default date range (2000-01-01 to today)
python -m ingest all --start 2000-01-01 --max-workers 10
```

This will:
1. Download S&P 500 membership (~500 tickers)
2. Download OHLCV data for all tickers
3. Download dividends and splits
4. Download fundamentals (statements, ratios, shares, classifications)
5. Download macro indicators

**Estimated time**: 2-4 hours for full historical data (depends on API limits and network speed)

### Option 2: Step-by-Step

```bash
# Step 1: Get S&P 500 membership (fast, ~30 seconds)
python -m ingest sp500-membership

# Step 2: Get OHLCV data (slower, ~30-60 min for 500 tickers)
python -m ingest ohlcv --start 2000-01-01 --max-workers 10

# Step 3: Get corporate actions (moderate, ~15-30 min)
python -m ingest corporate-actions --start 2000-01-01 --max-workers 10

# Step 4: Get fundamentals (moderate, ~20-40 min)
python -m ingest fundamentals --max-workers 3

# Step 5: Get macro data (fast, ~2 minutes)
python -m ingest macro --start 2000-01-01
```

## Dry Run (Test First)

```bash
# Preview what would be downloaded without actually writing data
python -m ingest all --start 2000-01-01 --dry-run
```

## Incremental Updates (Daily/Weekly)

Once you have historical data, update incrementally:

```bash
# Update only new data (much faster)
python -m ingest all --start 2025-01-01
```

This will:
- Update membership (add/remove constituents)
- Download recent OHLCV data
- Get new dividends/splits
- Update fundamentals

**Estimated time**: 10-20 minutes

## Validate Your Data

```bash
# Run validation checks
python scripts/validate_data.py
```

This will check:
- Row counts and date ranges
- Missing values
- Known events (e.g., AAPL 4:1 split)
- Ingestion log for errors

## Quick Checks in Python

```python
import pandas as pd

# Check membership
membership = pd.read_parquet("equities/index_membership/sp500.parquet")
print(f"Current S&P 500 members: {membership['end_date'].isna().sum()}")

# Check OHLCV for a specific ticker
import pyarrow.parquet as pq
# Find AAPL data (in NASDAQ partition)
aapl = pd.read_parquet("equities/ohlcv/daily/us/NASDAQ/NASDAQ.parquet")
aapl = aapl[aapl['ticker'] == 'AAPL.US']
print(f"AAPL: {len(aapl)} days from {aapl['date'].min()} to {aapl['date'].max()}")

# Check latest fundamentals
annual = pd.read_parquet("equities/fundamentals/annual/statements.parquet")
aapl_annual = annual[annual['ticker'] == 'AAPL.US'].sort_values('period_end_date')
print(f"Latest AAPL revenue: ${aapl_annual['revenue'].iloc[-1]/1e9:.1f}B")
```

## Common Issues

**"EODHD_API_KEY not found"**
- Create a `.env` file in the project root
- Add: `EODHD_API_KEY=your_key`

**Rate limit errors**
- Reduce `--max-workers` (try 3 or 5)
- Edit `config/ingest.yaml` to lower `requests_per_second`

**No data for a ticker**
- Ensure it was in the S&P 500 during your date range
- Check membership: `pd.read_parquet("equities/index_membership/sp500.parquet")`

## Next Steps

1. **Explore the data**: Use Jupyter notebooks or pandas scripts
2. **Schedule updates**: Use cron or Airflow to run `python -m ingest all` daily/weekly
3. **Build strategies**: Use your clean, survivorship-bias-free dataset for backtesting

## File Locations

```
equities/
  ├── ohlcv/daily/us/{NASDAQ,NYSE,AMEX}/*.parquet  # OHLCV data
  ├── index_membership/sp500.parquet                # Membership history
  ├── corporate-actions/
  │   ├── dividends.parquet                         # Dividend history
  │   └── splits.parquet                            # Split history
  └── fundamentals/
      ├── annual/statements.parquet                 # Annual financials
      ├── quarterly/statements.parquet              # Quarterly financials
      ├── ratios/ratios.parquet                     # Valuation ratios
      ├── shares-outstanding/shares.parquet         # Shares outstanding
      └── classifications/classifications.parquet   # Sector/industry

macro/
  ├── rates/rates.parquet                           # Interest rates
  ├── inflation/inflation.parquet                   # CPI, etc.
  └── growth/growth.parquet                         # GDP, etc.

_meta/
  ├── ingestion-log.parquet                         # All ingestion runs
  └── ticker-mapping.parquet                        # Ticker normalization
```

## Pro Tips

1. **Start with a subset**: Test with `--tickers AAPL.US MSFT.US GOOGL.US` first
2. **Use dry-run**: Always test with `--dry-run` before full runs
3. **Check logs**: Review `_meta/ingestion-log.parquet` for failures
4. **Monitor progress**: The CLI shows progress bars and summaries
5. **Partition awareness**: OHLCV is partitioned by exchange for faster queries

## Support

- Check `README.md` for detailed documentation
- Review `config/ingest.yaml` for all configuration options
- Inspect `ingest/common/schema.py` for data schemas
- See EODHD API docs: https://eodhd.com/financial-apis/
