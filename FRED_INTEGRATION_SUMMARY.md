# FRED Integration - Implementation Summary

## What's Changed

The project now uses the **FRED API** (Federal Reserve Economic Data) directly for macroeconomic data instead of EODHD. This provides access to 800,000+ authoritative economic time series.

## Files Created

### 1. `ingest/common/fred_client.py` (NEW)
- Full-featured FRED API client
- Rate limiting (120 requests/minute)
- Automatic retries with exponential backoff
- Support for series observations, metadata, and search
- Context manager support for clean resource handling

Key methods:
- `get_series_observations()` - Get time series data
- `get_series_info()` - Get series metadata
- `search_series()` - Search for series by text

### 2. `FRED_GUIDE.md` (NEW)
- Comprehensive guide to using FRED integration
- 50+ example series IDs across all categories
- Usage examples and best practices
- Data frequency and transformation documentation
- Common series combinations for different analyses

## Files Modified

### 1. `ingest/ingest_macro.py`
**Changes:**
- Replaced `EODHDClient` with `FREDClient`
- Updated `parse_macro_data()` → `parse_fred_data()` to handle FRED's response format
- FRED uses `.` for missing values (converted to NaN)
- Updated environment variable from `EODHD_API_KEY` to `FRED_API_KEY`
- Updated docstrings to reflect FRED source

### 2. `config/ingest.yaml`
**Changes:**
- Restructured API configuration to support multiple providers
- Added `api.fred` section with FRED-specific settings
- Expanded macro series list from 5 to 40+ series
- Added new categories: `employment`
- Kept backward compatibility with legacy `api.*` fields

New series categories:
- **Interest Rates**: Treasury yields (3M, 2Y, 5Y, 10Y, 30Y), spreads, Fed Funds, mortgage rates, corporate bonds
- **Inflation**: CPI, core CPI, PCE, core PCE, breakeven inflation
- **Growth**: GDP, real GDP, potential GDP, industrial production, sentiment, VIX, oil
- **Employment**: Unemployment (U3, U6), payrolls, participation rate, jobless claims, wages

### 3. `README.md`
**Changes:**
- Updated project description to mention both EODHD and FRED
- Added FRED API key requirement
- Updated data sources section
- Added FRED usage examples
- Updated troubleshooting section
- Added acknowledgment for FRED/St. Louis Fed

## Configuration Structure

```yaml
api:
  eodhd:  # For equities data
    base_url: "https://eodhd.com/api"
    timeout: 30
    max_retries: 3
    retry_backoff_factor: 2.0
    rate_limit:
      requests_per_second: 10
      burst_size: 20
  
  fred:  # For macro data
    base_url: "https://api.stlouisfed.org/fred"
    timeout: 30
    max_retries: 3
    retry_backoff_factor: 2.0
    rate_limit:
      requests_per_minute: 120
```

## Environment Variables

You now need TWO API keys:

```bash
# .env file
EODHD_API_KEY=your_eodhd_key_here    # For equities (OHLCV, fundamentals, etc.)
FRED_API_KEY=your_fred_key_here       # For macro data (FREE)
```

## Getting a FRED API Key

1. Go to https://fred.stlouisfed.org/
2. Create free account
3. Visit https://fred.stlouisfed.org/docs/api/api_key.html
4. Request API key (instant approval)

## Usage

### Run macro ingestion (now uses FRED):
```bash
python -m ingest macro --start 2000-01-01
```

### Download specific series:
```bash
python -m ingest macro --start 2000-01-01 --series DGS10 UNRATE GDP VIXCLS
```

### Add new series:
Edit `config/ingest.yaml`:
```yaml
datasets:
  macro:
    series:
      - code: "BAMLH0A0HYM2"  # FRED series ID
        name: "high_yield_spread"
        category: "rates"
```

Find series at: https://fred.stlouisfed.org/search

## Benefits Over EODHD for Macro Data

| Feature | FRED | EODHD |
|---------|------|-------|
| Number of series | 800,000+ | Limited selection |
| Data quality | Authoritative (Fed) | Proxied |
| Update frequency | Real-time to annual | Varies |
| Cost | **FREE** | Requires subscription |
| Rate limit | 120 req/min | 10 req/sec |
| Metadata | Comprehensive | Basic |
| Transformations | Built-in (YoY%, logs, etc.) | Manual |
| Documentation | Excellent | Good |

## Data Output

No changes to output format - still stored in `macro/` directory by category:

```
macro/
├── rates/rates.parquet
├── inflation/inflation.parquet
├── growth/growth.parquet
└── employment/employment.parquet
```

Schema remains the same:
- `date` - Observation date
- `value` - Numeric value
- `series_code` - Series ID (FRED series ID)
- `series_name` - Descriptive name
- `category` - Category

## Backward Compatibility

- Data output format unchanged
- Schema unchanged
- File paths unchanged
- CLI commands unchanged
- Only need to:
  1. Get FRED API key (free)
  2. Add to `.env` file
  3. Run ingestion as before

## Examples of New Series Available

### Recession Indicators
- `T10Y2Y` - 10Y-2Y Treasury spread (yield curve)
- `BAMLH0A0HYM2` - High yield spread
- `UMCSENT` - Consumer sentiment

### Fed Policy
- `FEDFUNDS` - Federal Funds Rate
- `WALCL` - Fed balance sheet size
- `DGS2` - 2Y Treasury (most Fed-sensitive)

### Real Economy
- `INDPRO` - Industrial production
- `HOUST` - Housing starts
- `RRSFS` - Retail sales
- `PAYEMS` - Nonfarm payrolls

### Inflation
- `T5YIE`, `T10YIE` - Breakeven inflation (market expectations)
- `PCEPILFE` - Core PCE (Fed's preferred gauge)

### Market Stress
- `VIXCLS` - VIX volatility index
- `DCOILWTICO` - WTI crude oil

## Testing

To test the integration:

```bash
# 1. Verify API key is set
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('FRED_API_KEY'))"

# 2. Test with dry run
python -m ingest macro --start 2020-01-01 --dry-run

# 3. Download a few series
python -m ingest macro --start 2020-01-01 --series DGS10 UNRATE

# 4. Verify data
python -c "import pandas as pd; df=pd.read_parquet('macro/rates/rates.parquet'); print(df.head())"
```

## Next Steps

1. **Get FRED API key** (free, takes 30 seconds)
2. **Add to `.env`** file
3. **Run ingestion** with your desired date range
4. **Explore series** at https://fred.stlouisfed.org/
5. **Add custom series** to `config/ingest.yaml`

## Resources

- **FRED Website**: https://fred.stlouisfed.org/
- **API Docs**: https://fred.stlouisfed.org/docs/api/
- **Series Search**: https://fred.stlouisfed.org/search
- **This Project's Guide**: See `FRED_GUIDE.md`

## Support

If you encounter issues:
1. Check API key is set in `.env`
2. Verify series IDs at fred.stlouisfed.org
3. Use `--dry-run` to test without writing data
4. Check rate limits if getting 429 errors

---

**Migration Complete!** Your project now has enterprise-grade macro data access through FRED. 🎉
