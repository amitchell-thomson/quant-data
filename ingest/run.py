"""Ingestion runner: pull a configured dataset from its provider and land it as parquet.

The clients, schemas and parquet upsert already existed; what was missing was the thing that
drives them. This is it. Everything it knows comes from ``config/datasets.yaml`` — adding a
dataset is a config change, not a code change.

Design notes worth knowing before changing anything:

- **Idempotent by default.** A dataset whose output file already exists is skipped unless
  ``--force`` is passed. Re-running the whole config after adding one dataset therefore costs
  nothing, which is what makes it safe to run from cron.
- **Year-partitioned when the output path says so.** An output containing ``{year}`` is fetched one
  year at a time and written to one file per year. This keeps a 40-year daily file from having to
  fit in memory, and means an interrupted run resumes at the year it reached.
- **Append-only via upsert.** New rows win on primary-key conflicts; nothing is silently dropped
  except genuine duplicate keys, which are logged as a row-count delta.
- **Every run is logged** to ``_meta/ingestion-log.parquet`` — dataset, year, rows, duration,
  status, and the error if it failed. A partial failure does not abort the rest of the run,
  because an overnight pull of twelve datasets should not be lost to one bad year.

Usage::

    python -m ingest --list
    python -m ingest --dataset corporate_actions.ma_deals
    python -m ingest --dataset equities.daily --years 2015-2026
    python -m ingest --all --dry-run
"""

from __future__ import annotations

import argparse
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingest.common.config_utils import _get_dataset, _load_config
from ingest.common.io_parquet import upsert_parquet
from ingest.common.provider import get_client
from ingest.common.schema import load_schema

# Importing the clients is what runs their @register decorators; without this the provider
# registry is empty and every dataset fails with "Provider 'wrds' is not registered".
from ingest.common import fred_client, wrds_client  # noqa: F401,E402  (side effect: registration)

DEFAULT_START_YEAR = 1985


def data_root() -> Path:
    return Path(_load_config("ingest")["data_root"])


def log_path() -> Path:
    cfg = _load_config("ingest")
    return data_root() / cfg.get("logging", {}).get(
        "ingestion_log_path", "_meta/ingestion-log.parquet"
    )


def all_dataset_keys() -> list[str]:
    """Every ``section.name`` in datasets.yaml, in file order."""
    cfg = _load_config("datasets")
    return [
        f"{section}.{name}"
        for section, entries in cfg.items()
        if isinstance(entries, dict)
        for name, body in entries.items()
        if isinstance(body, dict) and "output" in body
    ]


def _years(cfg: dict, requested: str | None) -> list[int | None]:
    """Which years to fetch. ``[None]`` means the dataset is not year-partitioned."""
    if "{year}" not in cfg["output"]:
        return [None]
    if requested:
        if "-" in requested:
            first, last = requested.split("-", 1)
            return list(range(int(first), int(last) + 1))
        return [int(requested)]
    start = int(cfg.get("start_year", DEFAULT_START_YEAR))
    return list(range(start, datetime.now(timezone.utc).year + 1))


def _record(rows: list[dict]) -> None:
    """Append run records to the ingestion log, creating it on first use."""
    if not rows:
        return
    path = log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    if path.exists():
        frame = pd.concat([pd.read_parquet(path), frame], ignore_index=True)
    frame.to_parquet(path, index=False, compression="snappy")


def ingest(
    key: str,
    *,
    years: str | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> list[dict]:
    """Fetch one dataset and write it. Returns one record per file written or skipped."""
    cfg = _get_dataset(key)
    schema, required, primary_key = load_schema(key)
    providers = _load_config("providers")
    provider = cfg["provider"]

    records: list[dict] = []
    client = None
    try:
        for year in _years(cfg, years):
            relative = cfg["output"].format(year=year) if year is not None else cfg["output"]
            destination = data_root() / relative

            if destination.exists() and not force:
                print(f"  skip   {relative}  (exists; --force to rebuild)")
                continue
            if dry_run:
                print(f"  would  {relative}")
                records.append(
                    {"dataset": key, "year": year, "path": relative, "status": "dry-run",
                     "rows": 0, "seconds": 0.0, "error": "",
                     "run_at": datetime.now(timezone.utc)}
                )
                continue

            if client is None:
                client = get_client(provider, providers[provider])

            started = time.monotonic()
            try:
                frame = client.fetch(cfg, year=year)
                rows = len(frame)
                if rows:
                    upsert_parquet(frame, destination, schema, required, primary_key)
                    size = destination.stat().st_size / 1e6
                    status, note = "ok", f"{rows:,} rows, {size:.1f}MB"
                else:
                    status, note = "empty", "no rows returned"
                elapsed = time.monotonic() - started
                print(f"  {status:<6} {relative}  {note}  [{elapsed:.1f}s]")
                records.append(
                    {"dataset": key, "year": year, "path": relative, "status": status,
                     "rows": rows, "seconds": round(elapsed, 2), "error": "",
                     "run_at": datetime.now(timezone.utc)}
                )
            except Exception as exc:  # one bad year must not lose the rest of the run
                elapsed = time.monotonic() - started
                print(f"  FAILED {relative}  {type(exc).__name__}: {str(exc)[:120]}")
                records.append(
                    {"dataset": key, "year": year, "path": relative, "status": "failed",
                     "rows": 0, "seconds": round(elapsed, 2),
                     "error": f"{type(exc).__name__}: {exc}"[:500],
                     "run_at": datetime.now(timezone.utc)}
                )
    finally:
        if client is not None and hasattr(client, "close"):
            client.close()

    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest configured datasets into the data root.")
    parser.add_argument("--dataset", action="append", help="section.name; repeatable")
    parser.add_argument("--all", action="store_true", help="every dataset in datasets.yaml")
    parser.add_argument("--list", action="store_true", help="show configured datasets and status")
    parser.add_argument("--years", help="YYYY or YYYY-YYYY; year-partitioned datasets only")
    parser.add_argument("--force", action="store_true", help="rebuild files that already exist")
    parser.add_argument("--dry-run", action="store_true", help="say what would be written")
    args = parser.parse_args(argv)

    if args.list:
        root = data_root()
        print(f"data_root: {root}\n")
        for key in all_dataset_keys():
            cfg = _get_dataset(key)
            template = cfg["output"]
            if "{year}" in template:
                parent = (root / template).parent
                have = len(list(parent.glob("*.parquet"))) if parent.exists() else 0
                state = f"{have} year files" if have else "empty"
            else:
                target = root / template
                state = f"{target.stat().st_size / 1e6:.1f}MB" if target.exists() else "empty"
            print(f"  {key:<38} {cfg['provider']:<6} {state:<16} {template}")
        return 0

    keys = all_dataset_keys() if args.all else (args.dataset or [])
    if not keys:
        parser.error("choose --dataset, --all, or --list")

    records: list[dict] = []
    for key in keys:
        print(f"\n{key}")
        try:
            records += ingest(key, years=args.years, force=args.force, dry_run=args.dry_run)
        except Exception as exc:
            print(f"  FAILED to start: {type(exc).__name__}: {exc}")
            traceback.print_exc(limit=2)
            records.append(
                {"dataset": key, "year": None, "path": "", "status": "failed", "rows": 0,
                 "seconds": 0.0, "error": f"{type(exc).__name__}: {exc}"[:500],
                 "run_at": datetime.now(timezone.utc)}
            )

    if not args.dry_run:
        _record(records)

    failed = [r for r in records if r["status"] == "failed"]
    total = sum(r["rows"] for r in records)
    print(f"\n{len(records)} file(s) touched, {total:,} rows, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
