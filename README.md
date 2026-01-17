# Quant Data Ingestion Pipeline

A production-grade data ingestion system for financial market data from EODHD (End of Day Historical Data). Designed for survivorship-bias-safe, append-only data collection suitable for quantitative research and backtesting.

## Features

- **Survivorship-Bias-Safe**: Ingests data for all tickers (including delisted) based on historical S&P 500 membership
- **Append-Only**: Immutable data storage with idempotent upserts (never silently overwrites raw data)
- **Resilient**: Automatic retries with exponential backoff, rate limiting, and comprehensive error handling
- **Reproducible**: Schema versioning, ingestion logs, and deterministic outputs
- **Production-Ready**: Type hints, comprehensive logging, progress bars, dry-run mode
- **Efficient**: Parallel downloads, Parquet compression, partitioned storage

## Data Sources

### From EODHD API

1. **S&P 500 Membership**: Current constituents with sector/industry classifications
2. **OHLCV Data**: Daily open/high/low/close/volume history (back to earliest available)
3. **Corporate Actions**: Dividends and stock splits
4. **Fundamentals**: 
   - Annual and quarterly financial statements (income, balance sheet, cash flow)
   - Key ratios and metrics (P/E, P/B, ROE, margins, etc.)
   - Shares outstanding and market cap
   - Company classifications
5. **Macroeconomic Data**: Interest rates, inflation, GDP, etc.

## Project Structure

```
quant-data/
├── config/
│   └── ingest.yaml              # Configuration for data ingestion
├── ingest/                       # Ingestion package
│   ├── common/                   # Shared utilities
│   │   ├── eodhd_client.py      # EODHD API client with retries/rate limiting
│   │   ├── io_parquet.py        # Parquet I/O with upsert logic
│   │   ├── logging.py           # Ingestion logging
│   │   └── schema.py            # Schema definitions
│   ├── ingest_sp500_membership.py
│   ├── ingest_ohlcv.py
│   ├── ingest_corporate_actions.py
│   ├── ingest_fundamentals.py
│   ├── ingest_macro.py
│   └── __main__.py              # CLI entrypoint
├── equities/                     # Market data storage
│   ├── ohlcv/daily/us/          # OHLCV partitioned by exchange
│   ├── index_membership/        # S&P 500 membership history
│   ├── corporate-actions/       # Dividends and splits
│   └── fundamentals/            # Financial statements and ratios
├── macro/                        # Macroeconomic data
├── _meta/                        # Metadata and logs
│   ├── ingestion-log.parquet    # Log of all ingestion runs
│   ├── ticker-mapping.parquet   # Ticker normalization mapping
│   └── schema-versions/         # Schema version history
├── pyproject.toml
└── README.md
```

## Installation

### Requirements

- Python 3.11+
- EODHD API key (get one at [eodhd.com](https://eodhd.com/))

### Setup

1. Clone or navigate to the project directory:
   ```bash
   cd /Users/alecmitchell-thomson/Desktop/Coding/quant-data
   ```

2. Install dependencies:
   ```bash
   pip install -e .
   ```

3. Create a `.env` file with your API key:
   ```bash
   cp .env.example .env
   # Edit .env and add your API key
   ```

4. (Optional) Adjust paths in `config/ingest.yaml` if needed.

## Usage

### Quick Start

```bash
# 1. Update S&P 500 membership (always run first)
python -m ingest sp500-membership

# 2. Download OHLCV data from 2000 onwards
python -m ingest ohlcv --start 2000-01-01

# 3. Download corporate actions
python -m ingest corporate-actions --start 2000-01-01

# 4. Download fundamentals
python -m ingest fundamentals

# 5. (Optional) Download macro data
python -m ingest macro --start 2000-01-01
```

### CLI Commands

#### S&P 500 Membership
```bash
python -m ingest sp500-membership [--dry-run] [--force]
```

#### OHLCV Data
```bash
python -m ingest ohlcv \
  --start 2000-01-01 \
  --end 2025-12-31 \
  --max-workers 10 \
  [--tickers AAPL.US MSFT.US] \
  [--dry-run] [--force]
```

#### Corporate Actions (Dividends & Splits)
```bash
python -m ingest corporate-actions \
  --start 2000-01-01 \
  --max-workers 10 \
  [--dividends-only] [--splits-only] \
  [--dry-run]
```

#### Fundamentals
```bash
python -m ingest fundamentals \
  --max-workers 3 \
  [--annual] [--quarterly] [--ratios] [--shares] [--classifications] \
  [--tickers AAPL.US MSFT.US] \
  [--dry-run]
```

#### Macroeconomic Data
```bash
python -m ingest macro \
  --start 2000-01-01 \
  [--series DGS10 FEDFUNDS] \
  [--dry-run]
```

#### Run Full Pipeline
```bash
python -m ingest all \
  --start 2000-01-01 \
  --max-workers 10 \
  [--skip-membership] [--skip-ohlcv] [--skip-actions] \
  [--skip-fundamentals] [--skip-macro] \
  [--dry-run]
```

### Common Options

- `--dry-run`: Preview what would be downloaded without writing data
- `--force`: Force re-download even if data exists
- `--max-workers N`: Number of parallel workers for downloads
- `--start DATE`: Start date for historical data (YYYY-MM-DD)
- `--end DATE`: End date (YYYY-MM-DD, defaults to today)
- `--tickers T1 T2`: Download specific tickers only

## Data Quality Validation

### Automated Checks

The pipeline includes:
- Schema validation (enforces dtypes and required columns)
- Primary key deduplication
- Missing value detection
- Ingestion logging (tracks all runs with status and row counts)

### Manual Validation Checklist

After running the ingestion, verify:

1. **Membership Data**:
   ```python
   import pandas as pd
   membership = pd.read_parquet("equities/index_membership/sp500.parquet")
   print(f"Total constituents: {len(membership)}")
   print(f"Current members: {membership['end_date'].isna().sum()}")
   ```

2. **OHLCV Data**:
   ```python
   # Check a known ticker
   aapl = pd.read_parquet("equities/ohlcv/daily/us/NASDAQ/NASDAQ.parquet")
   aapl = aapl[aapl['ticker'] == 'AAPL.US']
   print(f"AAPL rows: {len(aapl)}")
   print(f"Date range: {aapl['date'].min()} to {aapl['date'].max()}")
   print(f"Missing values:\n{aapl[['open','high','low','close','volume']].isna().sum()}")
   ```

3. **Corporate Actions**:
   ```python
   dividends = pd.read_parquet("equities/corporate-actions/dividends.parquet")
   splits = pd.read_parquet("equities/corporate-actions/splits.parquet")
   print(f"Total dividends: {len(dividends)}")
   print(f"Total splits: {len(splits)}")
   
   # Spot check known events (e.g., AAPL 4:1 split on 2020-08-31)
   aapl_splits = splits[splits['ticker'] == 'AAPL.US']
   print(aapl_splits)
   ```

4. **Fundamentals**:
   ```python
   annual = pd.read_parquet("equities/fundamentals/annual/statements.parquet")
   print(f"Total annual statements: {len(annual)}")
   print(f"Tickers with data: {annual['ticker'].nunique()}")
   
   # Check a known ticker
   aapl_annual = annual[annual['ticker'] == 'AAPL.US'].sort_values('period_end_date')
   print(f"AAPL annual statements: {len(aapl_annual)}")
   print(aapl_annual[['period_end_date', 'revenue', 'net_income']].tail())
   ```

5. **Ingestion Log**:
   ```python
   log = pd.read_parquet("_meta/ingestion-log.parquet")
   print(log.groupby(['dataset', 'status']).size())
   print(f"\nLast run: {log['timestamp'].max()}")
   ```

## Configuration

Edit `config/ingest.yaml` to customize:

- Data paths
- Date ranges
- API settings (rate limits, timeouts, retries)
- Concurrency (max workers, batch sizes)
- Dataset-specific options (partitioning, required fields)

## Schema Versioning

Schemas are defined in `ingest/common/schema.py` with version `1.0.0`. Each schema includes:
- Field names and dtypes (via PyArrow)
- Required columns
- Primary keys (for deduplication)

Schema versions are saved to `_meta/schema-versions/` for reproducibility.

## Handling Delisted Tickers

EODHD appends `_old` to delisted ticker symbols (e.g., `AAPL.US_old`). The pipeline:
- Tracks delisted status in `_meta/ticker-mapping.parquet`
- Normalizes tickers by stripping `_old` suffix
- Ensures historical data for delisted tickers is retained

## Idempotency & Append-Only Design

- **Upsert Logic**: New data is merged with existing data based on primary keys
- **Conflict Resolution**: On conflicts, new data overwrites old (for corrections)
- **No Silent Overwrites**: Existing files are read → merged → written back
- **Ingestion Log**: Every run is logged with status, row counts, and timestamps

## Performance & Rate Limits

- **Default Settings**: 10 requests/second, burst size 20
- **Parallel Workers**: 5 for OHLCV/actions, 3 for fundamentals (heavier requests)
- **Retry Strategy**: Exponential backoff (2, 4, 8, 16, 32 seconds)
- **Connection Pooling**: HTTP session reuse for efficiency

Adjust in `config/ingest.yaml`:
```yaml
api:
  rate_limit:
    requests_per_second: 10
    burst_size: 20

concurrency:
  max_workers: 5
```

## Extending the Pipeline

### Adding New Data Sources

1. Create a new ingestion script (e.g., `ingest_options.py`)
2. Define schema in `common/schema.py`
3. Add client methods to `common/eodhd_client.py` if needed
4. Wire up in `__main__.py` CLI

### Adding New Macro Series

Edit `config/ingest.yaml`:
```yaml
datasets:
  macro:
    series:
      - code: "NEW_SERIES"
        name: "Description"
        category: "rates"  # or inflation, growth, etc.
```

## Troubleshooting

### Common Issues

1. **API Key Not Found**:
   - Ensure `.env` file exists with `EODHD_API_KEY=your_key`
   - Run `python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('EODHD_API_KEY'))"`

2. **Rate Limit Errors**:
   - Reduce `requests_per_second` in `config/ingest.yaml`
   - Reduce `max_workers` for parallel downloads

3. **No Data Returned for Ticker**:
   - Check ticker format (should include exchange, e.g., `AAPL.US`)
   - Verify ticker was in S&P 500 during the date range
   - Check EODHD API directly for data availability

4. **Schema Validation Errors**:
   - Review error message for missing/incorrect columns
   - Check `common/schema.py` for required fields
   - Use `--dry-run` to preview data before writing

### Debug Mode

Set log level to DEBUG in `config/ingest.yaml`:
```yaml
logging:
  log_level: "DEBUG"
```

Or inspect the ingestion log:
```python
import pandas as pd
log = pd.read_parquet("_meta/ingestion-log.parquet")
failed = log[log['status'] == 'failed']
print(failed[['ticker', 'dataset', 'error_message']])
```

## License

This project is for personal use. Data from EODHD is subject to their terms of service.

## Acknowledgments

- Data provided by [EODHD](https://eodhd.com/)
- Built with Pandas, PyArrow, and Requests
