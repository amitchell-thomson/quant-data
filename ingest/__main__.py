"""
CLI entrypoint for the ingestion pipeline.

Usage:
    python -m ingest sp500-membership
    python -m ingest ohlcv --start 2000-01-01
    python -m ingest corporate-actions
    python -m ingest fundamentals --quarterly --annual
    python -m ingest macro                           # Uses FRED API
    python -m ingest all
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def main():
    """Main CLI entrypoint."""
    # Load environment variables
    load_dotenv()

    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        print("Error: EODHD_API_KEY not found in environment")
        print("Please create a .env file with your API key:")
        print("  EODHD_API_KEY=your_key_here")
        sys.exit(1)

    # Default config path
    config_path = Path(__file__).parent.parent / "config" / "ingest.yaml"

    # Parse command
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1]

    # Remove command from sys.argv for argparse in sub-modules
    sys.argv = [sys.argv[0]] + sys.argv[2:]

    # Route to appropriate ingestion module
    if command == "sp500-membership" or command == "membership":
        import argparse

        from .ingest_sp500_membership import ingest_sp500_membership

        parser = argparse.ArgumentParser(description="Ingest S&P 500 membership")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        parser.add_argument("--force", action="store_true", help="Force re-download")
        args = parser.parse_args()

        ingest_sp500_membership(
            config_path=config_path,
            api_key=api_key,
            dry_run=args.dry_run,
            force=args.force,
        )

    elif command == "ohlcv":
        import argparse

        from .ingest_ohlcv import ingest_ohlcv

        parser = argparse.ArgumentParser(description="Ingest OHLCV data")
        parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end", help="End date (YYYY-MM-DD)")
        parser.add_argument("--tickers", nargs="+", help="Specific tickers")
        parser.add_argument("--max-workers", type=int, default=5, help="Parallel workers")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        parser.add_argument("--force", action="store_true", help="Force re-download")
        args = parser.parse_args()

        ingest_ohlcv(
            config_path=config_path,
            api_key=api_key,
            start_date=args.start,
            end_date=args.end,
            tickers=args.tickers,
            max_workers=args.max_workers,
            dry_run=args.dry_run,
            force=args.force,
        )

    elif command == "corporate-actions" or command == "actions":
        import argparse

        from .ingest_corporate_actions import ingest_corporate_actions

        parser = argparse.ArgumentParser(description="Ingest corporate actions")
        parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end", help="End date (YYYY-MM-DD)")
        parser.add_argument("--tickers", nargs="+", help="Specific tickers")
        parser.add_argument("--max-workers", type=int, default=5, help="Parallel workers")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        parser.add_argument("--force", action="store_true", help="Force re-download")
        parser.add_argument("--dividends-only", action="store_true", help="Only dividends")
        parser.add_argument("--splits-only", action="store_true", help="Only splits")
        args = parser.parse_args()

        ingest_corporate_actions(
            config_path=config_path,
            api_key=api_key,
            start_date=args.start,
            end_date=args.end,
            tickers=args.tickers,
            max_workers=args.max_workers,
            dry_run=args.dry_run,
            force=args.force,
            dividends_only=args.dividends_only,
            splits_only=args.splits_only,
        )

    elif command == "fundamentals":
        import argparse

        from .ingest_fundamentals import ingest_fundamentals

        parser = argparse.ArgumentParser(description="Ingest fundamental data")
        parser.add_argument("--tickers", nargs="+", help="Specific tickers")
        parser.add_argument("--max-workers", type=int, default=3, help="Parallel workers")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
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

    elif command == "macro":
        from .ingest_macro import ingest_macro

        # Macro uses FRED API, not EODHD
        fred_api_key = os.getenv("FRED_API_KEY")
        if not fred_api_key:
            print("Error: FRED_API_KEY not found in environment")
            print("Get a free API key at https://fred.stlouisfed.org/docs/api/api_key.html")
            print("Add to your .env file: FRED_API_KEY=your_key_here")
            sys.exit(1)

        import argparse

        parser = argparse.ArgumentParser(description="Ingest macro indicators from FRED")
        parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end", help="End date (YYYY-MM-DD)")
        parser.add_argument("--series", nargs="+", help="Specific FRED series IDs")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        parser.add_argument("--force", action="store_true", help="Force re-download")
        args = parser.parse_args()

        ingest_macro(
            config_path=config_path,
            api_key=fred_api_key,
            start_date=args.start,
            end_date=args.end,
            series=args.series,
            dry_run=args.dry_run,
            force=args.force,
        )

    elif command == "macro-alfred":
        from .ingest_macro_alfred import ingest_macro_alfred

        # ALFRED uses the same FRED API key
        fred_api_key = os.getenv("FRED_API_KEY")
        if not fred_api_key:
            print("Error: FRED_API_KEY not found in environment")
            print("Get a free API key at https://fred.stlouisfed.org/docs/api/api_key.html")
            print("Add to your .env file: FRED_API_KEY=your_key_here")
            sys.exit(1)

        import argparse

        parser = argparse.ArgumentParser(
            description="Ingest archival vintage macro indicators from ALFRED"
        )
        parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end", help="End date (YYYY-MM-DD)")
        parser.add_argument("--series", nargs="+", help="Specific FRED series IDs")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        parser.add_argument("--force", action="store_true", help="Force re-download")
        args = parser.parse_args()

        ingest_macro_alfred(
            config_path=config_path,
            api_key=fred_api_key,
            start_date=args.start,
            end_date=args.end,
            series=args.series,
            dry_run=args.dry_run,
            force=args.force,
        )

    elif command == "all":
        print("\n" + "=" * 60)
        print("Running Full Ingestion Pipeline")
        print("=" * 60 + "\n")

        import argparse

        from .ingest_corporate_actions import ingest_corporate_actions
        from .ingest_fundamentals import ingest_fundamentals
        from .ingest_macro import ingest_macro
        from .ingest_macro_alfred import ingest_macro_alfred
        from .ingest_ohlcv import ingest_ohlcv
        from .ingest_sp500_membership import ingest_sp500_membership

        parser = argparse.ArgumentParser(description="Run full ingestion pipeline")
        parser.add_argument("--start", help="Start date for historical data (YYYY-MM-DD)")
        parser.add_argument("--end", help="End date (YYYY-MM-DD)")
        parser.add_argument("--max-workers", type=int, default=5, help="Parallel workers")
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        parser.add_argument("--skip-membership", action="store_true", help="Skip membership update")
        parser.add_argument("--skip-ohlcv", action="store_true", help="Skip OHLCV")
        parser.add_argument("--skip-actions", action="store_true", help="Skip corporate actions")
        parser.add_argument("--skip-fundamentals", action="store_true", help="Skip fundamentals")
        parser.add_argument("--skip-macro", action="store_true", help="Skip macro")
        parser.add_argument("--skip-macro-alfred", action="store_true", help="Skip ALFRED macro")
        args = parser.parse_args()

        try:
            # Step 1: Update membership (always first)
            if not args.skip_membership:
                print("\n[1/5] Updating S&P 500 membership...")
                ingest_sp500_membership(
                    config_path=config_path,
                    api_key=api_key,
                    dry_run=args.dry_run,
                )

            # Step 2: OHLCV data
            if not args.skip_ohlcv:
                print("\n[2/5] Ingesting OHLCV data...")
                ingest_ohlcv(
                    config_path=config_path,
                    api_key=api_key,
                    start_date=args.start,
                    end_date=args.end,
                    max_workers=args.max_workers,
                    dry_run=args.dry_run,
                )

            # Step 3: Corporate actions
            if not args.skip_actions:
                print("\n[3/5] Ingesting corporate actions...")
                ingest_corporate_actions(
                    config_path=config_path,
                    api_key=api_key,
                    start_date=args.start,
                    end_date=args.end,
                    max_workers=args.max_workers,
                    dry_run=args.dry_run,
                )

            # Step 4: Fundamentals
            if not args.skip_fundamentals:
                print("\n[4/5] Ingesting fundamentals...")
                ingest_fundamentals(
                    config_path=config_path,
                    api_key=api_key,
                    max_workers=max(1, args.max_workers // 2),  # Lighter load for fundamentals
                    dry_run=args.dry_run,
                )

            # Step 5: Macro data (uses FRED API)
            if not args.skip_macro:
                print("\n[5/6] Ingesting macro data...")
                fred_api_key = os.getenv("FRED_API_KEY")
                if not fred_api_key:
                    print("Warning: FRED_API_KEY not found, skipping macro data")
                    print("Get a free API key at https://fred.stlouisfed.org/docs/api/api_key.html")
                else:
                    ingest_macro(
                        config_path=config_path,
                        api_key=fred_api_key,
                        start_date=args.start,
                        end_date=args.end,
                        dry_run=args.dry_run,
                    )

            # Step 6: ALFRED archival vintage macro data
            if not args.skip_macro_alfred:
                print("\n[6/6] Ingesting ALFRED archival macro data...")
                fred_api_key = os.getenv("FRED_API_KEY")
                if not fred_api_key:
                    print("Warning: FRED_API_KEY not found, skipping ALFRED macro data")
                else:
                    ingest_macro_alfred(
                        config_path=config_path,
                        api_key=fred_api_key,
                        start_date=args.start,
                        end_date=args.end,
                        dry_run=args.dry_run,
                    )

            print("\n" + "=" * 60)
            print("✓ Full ingestion pipeline complete!")
            print("=" * 60 + "\n")

        except KeyboardInterrupt:
            print("\n\nIngestion interrupted by user.")
            sys.exit(1)
        except Exception as e:
            print(f"\n\nIngestion failed: {e}")
            sys.exit(1)

    else:
        print(f"Error: Unknown command '{command}'")
        print_usage()
        sys.exit(1)


def print_usage():
    """Print CLI usage information."""
    print("""
Financial Data Ingestion Pipeline

Usage:
    python -m ingest <command> [options]

Commands:
    sp500-membership    Download S&P 500 constituent list (EODHD)
    ohlcv               Download daily OHLCV data (EODHD)
    corporate-actions   Download dividends and splits (EODHD)
    fundamentals        Download financial statements and ratios (EODHD)
    macro               Download macroeconomic indicators (FRED)
    macro-alfred        Download archival vintage macro data (ALFRED)
    all                 Run full pipeline (all datasets)

Options:
    --start DATE        Start date (YYYY-MM-DD)
    --end DATE          End date (YYYY-MM-DD)
    --tickers T1 T2     Specific tickers to ingest
    --max-workers N     Number of parallel workers (default: 5)
    --dry-run           Preview without writing data
    --force             Force re-download even if data exists
    --log-level LEVEL   Logging level (DEBUG, INFO, WARNING, ERROR)

Examples:
    # Update S&P 500 membership
    python -m ingest sp500-membership

    # Download OHLCV data from 2000 onwards
    python -m ingest ohlcv --start 2000-01-01

    # Download specific tickers
    python -m ingest ohlcv --tickers AAPL.US MSFT.US --start 2020-01-01

    # Download corporate actions (dividends + splits)
    python -m ingest corporate-actions

    # Download fundamentals (all types)
    python -m ingest fundamentals

    # Download only quarterly statements
    python -m ingest fundamentals --quarterly

    # Download macro data from FRED
    python -m ingest macro --start 2000-01-01

    # Download specific FRED series
    python -m ingest macro --series DGS10 UNRATE GDP

    # Download archival vintage macro data (all historical revisions)
    python -m ingest macro-alfred --start 2000-01-01

    # Download ALFRED vintages for specific series only
    python -m ingest macro-alfred --series DGS10 GDP --dry-run

    # Run full pipeline
    python -m ingest all --start 2000-01-01 --max-workers 10

    # Dry run to preview
    python -m ingest all --dry-run

Environment:
    EODHD_API_KEY       EODHD API key (required for equities data, set in .env file)
    FRED_API_KEY        FRED API key (required for macro data, free at fred.stlouisfed.org)
""")


if __name__ == "__main__":
    main()
