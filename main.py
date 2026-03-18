"""SF Permitting Bottleneck Analyzer — Main entry point.

Usage:
    uv run python main.py fetch      # Download datasets from DataSF
    uv run python main.py clean      # Clean and process raw data
    uv run python main.py analyze    # Run analysis and print summary
    uv run python main.py report     # Generate HTML policy brief
    uv run python main.py dashboard  # Launch interactive Dash dashboard
    uv run python main.py pipeline   # Run full pipeline (fetch → clean → report → dashboard)
"""
import sys


def cmd_fetch():
    from src.ingestion.fetch import fetch_all
    data = fetch_all()
    for name, df in data.items():
        print(f"  {name}: {len(df):,} rows")


def cmd_clean():
    from src.ingestion.fetch import fetch_all
    from src.ingestion.clean import get_clean_data
    raw = fetch_all()  # Uses cache if available
    clean = get_clean_data(raw)
    for name, df in clean.items():
        print(f"  {name}: {len(df):,} rows (cleaned)")


def cmd_analyze():
    import pandas as pd
    from src.config import PROCESSED_DIR
    from src.analysis.bottlenecks import get_all_analyses, stuck_permits

    df = pd.read_parquet(PROCESSED_DIR / "building_permits.parquet")
    for col in ["filed_date", "approved_date", "issued_date", "completed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    results = get_all_analyses(df)
    stuck = stuck_permits(df)

    print("\n=== SF Permitting Bottleneck Analysis ===\n")
    housing = df[df["is_housing"] == True] if "is_housing" in df.columns else df
    print(f"Total housing permits: {len(housing):,}")
    print(f"Median days filed→issued: {housing['days_filed_to_issued'].median():.0f}")
    print(f"Stuck permits (>1yr): {len(stuck):,}")
    if "proposed_units" in stuck.columns:
        print(f"Housing units blocked: {stuck['proposed_units'].sum():,.0f}")
    print()

    for name, result in results.items():
        if hasattr(result, 'shape'):
            print(f"--- {name} ({result.shape[0]} rows) ---")
            print(result.head(10).to_string())
            print()


def cmd_report():
    import pandas as pd
    from src.config import PROCESSED_DIR
    from src.reports.generate import generate_report

    df = pd.read_parquet(PROCESSED_DIR / "building_permits.parquet")
    for col in ["filed_date", "approved_date", "issued_date", "completed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    generate_report(df)


def cmd_dashboard():
    from src.dashboard.app import create_app
    app = create_app()
    print("\nStarting dashboard at http://127.0.0.1:8050")
    print("Press Ctrl+C to stop.\n")
    app.run()


def cmd_pipeline():
    print("Step 1/4: Fetching data from DataSF...")
    cmd_fetch()
    print("\nStep 2/4: Cleaning data...")
    cmd_clean()
    print("\nStep 3/4: Generating report...")
    cmd_report()
    print("\nStep 4/4: Launching dashboard...")
    cmd_dashboard()


COMMANDS = {
    "fetch": cmd_fetch,
    "clean": cmd_clean,
    "analyze": cmd_analyze,
    "report": cmd_report,
    "dashboard": cmd_dashboard,
    "pipeline": cmd_pipeline,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
