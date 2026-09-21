# quant-data

Ingestion for WRDS and FRED. Pulls configured datasets and lands them as partitioned,
append-only Parquet under a single data root.

## Licence boundary — read this first

The WRDS subscription is **academic**. It permits **signal research, backtesting and validation.
It does not permit live trading in the loop.** WRDS logs every query against the account username.

The working pattern is:

> **Validate on WRDS. Trade on licence-clean live data.**

This works because the expensive half is *history* and the cheap half is *today*. Everything under
`wrds/` in the data root is research-only; anything intended for a live path must come from a
licence-clean source (EODHD, a broker feed) and live outside that prefix. The consuming project
(`nabla`) enforces this in code — `nabla.data.store` refuses to hand a `wrds/` path to anything
declaring a live purpose.

## Layout

`data_root` is `/srv/data`, a dedicated NVMe (`/dev/nvme0n1p1`, ~457G). Paths are set per dataset
in `config/datasets.yaml`:

```
/srv/data/
  wrds/crsp/      daily, events, delistings, names, index_membership, treasuries
  wrds/comp/      Compustat annual and quarterly
  wrds/sdc/       SDC Platinum M&A
  wrds/optionm/   OptionMetrics          (not ingested — see Scale)
  wrds/trace/     TRACE corporate bonds  (not ingested — see Scale)
  wrds/fisd/      Mergent FISD
  macro/fred/     rates, inflation, growth, employment, liquidity, fx, commodities
```

## Usage

```bash
set -a; . ./.env; set +a          # WRDS_USERNAME, FRED_API_KEY
.venv/bin/python -m ingest --list
.venv/bin/python -m ingest --dataset corporate_actions.ma_deals
.venv/bin/python -m ingest --dataset equities.daily --years 2015-2026
.venv/bin/python -m ingest --all --dry-run
```

Behaviour worth knowing:

- **Idempotent.** An output that already exists is skipped unless `--force` is given, so re-running
  the whole config after adding one dataset costs nothing.
- **Year-partitioned** when the configured `output` contains `{year}`: fetched a year at a time,
  one file per year, so a 40-year daily table never has to fit in memory and an interrupted run
  resumes where it stopped.
- **Upsert, not overwrite.** New rows win on primary-key conflicts.
- **Every run is logged** to `_meta/ingestion-log.parquet`. A failing dataset does not abort the
  rest of the run.

Adding a dataset is a change to `config/datasets.yaml` — provider, source table, output path,
primary key and typed columns — not a code change.

## Scale: two datasets are deliberately not ingested

- **OptionMetrics** (`options.daily`) — `optionm.opprcd2022` alone is **365M rows**, and the
  volatility surface for one year is **543M**. Twenty-five years is billions of rows and hundreds
  of gigabytes. It is configured but must be pulled selectively (specific underlyings, specific
  years), never with `--all`.
- **TRACE enhanced** (`fixed_income.trace`) — ~455M rows.

`--all` will attempt both. Prefer explicit `--dataset` for anything routine.

## Layout of the code

```
ingest/
  run.py            the runner: resolves config, fetches, upserts, logs
  __main__.py       CLI entry point
  common/
    provider.py     registry; clients self-register via @register
    base_client.py  client interface
    wrds_client.py  psycopg2 against wrds-pgdata (password from ~/.pgpass)
    fred_client.py  FRED REST
    schema.py       builds a pyarrow schema from the config's column types
    io_parquet.py   read / merge / validate / write
    config_utils.py loads config/*.yaml
```

Clients must be imported for their `@register` decorator to run; `run.py` imports them for that
side effect.
