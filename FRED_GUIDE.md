# FRED Integration Guide

This guide explains how to use the FRED (Federal Reserve Economic Data) API integration for downloading macroeconomic data.

## Overview

The pipeline now uses FRED directly instead of EODHD for macro data, giving you access to:
- **800,000+ economic time series** from the Federal Reserve
- **Higher data quality** - direct from authoritative sources
- **More frequent updates** - many series updated daily
- **Better metadata** - units, seasonal adjustment, source info
- **Free access** - up to 120 requests/minute

## Getting Started

### 1. Get a FRED API Key (Free)

1. Visit https://fred.stlouisfed.org/
2. Create a free account
3. Go to https://fred.stlouisfed.org/docs/api/api_key.html
4. Request an API key (instant approval)

### 2. Add to Your Environment

Add to your `.env` file:
```bash
FRED_API_KEY=your_fred_api_key_here
```

### 3. Run Macro Ingestion

```bash
python -m ingest macro --start 2000-01-01
```

## Finding FRED Series

### Search on FRED Website

1. Go to https://fred.stlouisfed.org/
2. Use the search bar (e.g., "unemployment rate")
3. Click on a series to view it
4. The series ID is in the URL (e.g., `UNRATE` from `https://fred.stlouisfed.org/series/UNRATE`)

### Common Categories

#### Interest Rates & Yields
- `DGS10` - 10-Year Treasury Constant Maturity Rate
- `DGS30` - 30-Year Treasury Rate
- `DGS5` - 5-Year Treasury Rate
- `DGS2` - 2-Year Treasury Rate
- `DGS3MO` - 3-Month Treasury Bill
- `T10Y2Y` - 10-Year minus 2-Year Treasury Spread
- `T10Y3M` - 10-Year minus 3-Month Treasury Spread
- `FEDFUNDS` - Federal Funds Effective Rate
- `MORTGAGE30US` - 30-Year Fixed Rate Mortgage Average
- `AAA` - Moody's Seasoned AAA Corporate Bond Yield
- `BAA` - Moody's Seasoned BAA Corporate Bond Yield
- `BAMLH0A0HYM2` - ICE BofA US High Yield Option-Adjusted Spread

#### Inflation & Prices
- `CPIAUCSL` - Consumer Price Index for All Urban Consumers (CPI-U)
- `CPILFESL` - Core CPI (excluding food and energy)
- `PCEPI` - Personal Consumption Expenditures Price Index
- `PCEPILFE` - Core PCE Price Index (Fed's preferred inflation gauge)
- `T5YIE` - 5-Year Breakeven Inflation Rate
- `T10YIE` - 10-Year Breakeven Inflation Rate
- `CORESTICKM159SFRBATL` - Atlanta Fed Sticky-Price CPI

#### Economic Growth & Activity
- `GDP` - Gross Domestic Product
- `GDPC1` - Real Gross Domestic Product
- `GDPPOT` - Real Potential GDP
- `INDPRO` - Industrial Production Index
- `UMCSENT` - University of Michigan Consumer Sentiment
- `DCOILWTICO` - Crude Oil Prices: West Texas Intermediate (WTI)
- `VIXCLS` - CBOE Volatility Index (VIX)
- `HOUST` - Housing Starts
- `PERMIT` - New Private Housing Units Authorized by Building Permits
- `RRSFS` - Retail and Food Services Sales

#### Employment & Labor Market
- `UNRATE` - Unemployment Rate
- `PAYEMS` - All Employees: Total Nonfarm Payrolls
- `CIVPART` - Labor Force Participation Rate
- `ICSA` - Initial Jobless Claims
- `U6RATE` - Total Unemployed Plus Marginally Attached Plus Part Time
- `AHETPI` - Average Hourly Earnings of Production and Nonsupervisory Employees
- `JTSJOL` - Job Openings: Total Nonfarm
- `EMRATIO` - Employment-Population Ratio

#### Money & Credit
- `M2SL` - M2 Money Stock
- `WALCL` - Fed Balance Sheet Total Assets
- `TOTALSL` - Total Consumer Credit Outstanding
- `DRTSCILM` - Net Percentage of Domestic Banks Tightening Standards

## Adding Series to Your Pipeline

Edit `config/ingest.yaml` under `datasets.macro.series`:

```yaml
datasets:
  macro:
    series:
      # Add your series here
      - code: "SERIES_ID"        # FRED series ID
        name: "descriptive_name"  # Snake_case name for your use
        category: "rates"         # rates, inflation, growth, employment
```

Example:
```yaml
      - code: "VIXCLS"
        name: "vix_volatility"
        category: "growth"
```

## Data Frequencies

FRED series come in different frequencies:
- **Daily** (d): Most interest rates, VIX, oil prices
- **Weekly** (w): Initial jobless claims
- **Monthly** (m): CPI, employment data, retail sales
- **Quarterly** (q): GDP, many corporate indicators
- **Annual** (a): Some long-term series

The FRED client can automatically convert frequencies using the `frequency` parameter:
```python
# In code (not needed for standard ingestion)
client.get_series_observations(
    series_id="GDP",
    frequency="m",              # Convert quarterly GDP to monthly
    aggregation_method="eop"    # End of period
)
```

## Units Transformations

FRED supports automatic transformations:
- `lin` - Levels (no transformation, default)
- `chg` - Change
- `ch1` - Change from year ago
- `pch` - Percent change
- `pc1` - Percent change from year ago
- `pca` - Compounded annual rate of change
- `cch` - Continuously compounded rate of change
- `cca` - Continuously compounded annual rate of change
- `log` - Natural log

Example in code:
```python
# Get year-over-year CPI change
client.get_series_observations(
    series_id="CPIAUCSL",
    units="pc1"  # Percent change from year ago
)
```

## Rate Limits

- **120 requests per minute** (FRED's limit)
- Configure in `config/ingest.yaml`:
  ```yaml
  api:
    fred:
      rate_limit:
        requests_per_minute: 120
  ```

## Data Storage

Macro data is stored by category in `macro/` directory:
```
macro/
├── rates/
│   └── rates.parquet
├── inflation/
│   └── inflation.parquet
├── growth/
│   └── growth.parquet
└── employment/
    └── employment.parquet
```

Each file contains all series for that category with schema:
- `date` - Observation date
- `value` - Numeric value
- `series_code` - FRED series ID
- `series_name` - Descriptive name
- `category` - Category (rates/inflation/growth/employment)

## Reading the Data

```python
import pandas as pd

# Load all rates data
rates = pd.read_parquet("macro/rates/rates.parquet")

# Filter to specific series
dgs10 = rates[rates['series_code'] == 'DGS10']

# Plot
dgs10.set_index('date')['value'].plot(title='10-Year Treasury Yield')
```

## Advanced: Using the FRED Client Directly

For custom analysis outside the pipeline:

```python
from ingest.common.fred_client import FREDClient
import os
from dotenv import load_dotenv

load_dotenv()
client = FREDClient(api_key=os.getenv('FRED_API_KEY'))

# Get data
data = client.get_series_observations(
    series_id="DGS10",
    observation_start="2020-01-01",
    observation_end="2024-01-01"
)

# Get metadata
info = client.get_series_info("DGS10")
print(info['title'])  # "10-Year Treasury Constant Maturity Rate"
print(info['units'])  # "Percent"
print(info['frequency'])  # "Daily"

# Search for series
results = client.search_series("unemployment", limit=10)
for series in results:
    print(f"{series['id']}: {series['title']}")

client.close()
```

## Useful Series Combinations

### Recession Indicators
- Yield curve inversion: `T10Y2Y` (negative = recession warning)
- Unemployment rate: `UNRATE`
- Consumer sentiment: `UMCSENT`
- High yield spread: `BAMLH0A0HYM2` (spikes during stress)

### Inflation Tracking
- CPI: `CPIAUCSL`
- Core CPI: `CPILFESL`
- PCE: `PCEPI`
- Breakeven inflation: `T5YIE`, `T10YIE`

### Fed Policy Analysis
- Fed Funds Rate: `FEDFUNDS`
- 2Y Treasury: `DGS2` (most sensitive to Fed)
- Fed Balance Sheet: `WALCL`
- Real rates: `DGS10` minus `T10YIE`

### Growth Indicators
- Real GDP: `GDPC1`
- Industrial Production: `INDPRO`
- Retail Sales: `RRSFS`
- Housing Starts: `HOUST`

## Resources

- **FRED Website**: https://fred.stlouisfed.org/
- **API Documentation**: https://fred.stlouisfed.org/docs/api/
- **Series Search**: https://fred.stlouisfed.org/search
- **Popular Series**: https://fred.stlouisfed.org/tags/series
- **Release Calendar**: https://fred.stlouisfed.org/releases/calendar

## Example: Building a Custom Macro Dataset

```yaml
# In config/ingest.yaml
datasets:
  macro:
    series:
      # Yield Curve
      - code: "DGS3MO"
        name: "3m_treasury"
        category: "rates"
      - code: "DGS2"
        name: "2y_treasury"
        category: "rates"
      - code: "DGS10"
        name: "10y_treasury"
        category: "rates"
      - code: "DGS30"
        name: "30y_treasury"
        category: "rates"
      
      # Credit Spreads
      - code: "BAA10Y"
        name: "baa_10y_spread"
        category: "rates"
      - code: "BAMLH0A0HYM2"
        name: "high_yield_spread"
        category: "rates"
      
      # Inflation Expectations
      - code: "T5YIE"
        name: "5y_inflation_expectation"
        category: "inflation"
      - code: "T10YIE"
        name: "10y_inflation_expectation"
        category: "inflation"
      
      # Real Economy
      - code: "UNRATE"
        name: "unemployment_rate"
        category: "employment"
      - code: "ICSA"
        name: "jobless_claims"
        category: "employment"
      - code: "INDPRO"
        name: "industrial_production"
        category: "growth"
      - code: "VIXCLS"
        name: "vix"
        category: "growth"
```

Then run:
```bash
python -m ingest macro --start 1990-01-01
```

This creates a comprehensive macro dataset ready for quantitative research!
