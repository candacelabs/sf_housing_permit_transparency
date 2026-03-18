"""SF Permitting Bottleneck Analyzer — Main entry point.

Usage:
    uv run python main.py fetch      # Download datasets from DataSF
    uv run python main.py clean      # Clean and process raw data
    uv run python main.py analyze    # Run analysis and print summary
    uv run python main.py report     # Generate HTML policy brief
    uv run python main.py dashboard  # Launch interactive Dash dashboard
    uv run python main.py pipeline   # Run full pipeline (fetch → clean → report → dashboard)
"""
import logging
import sys

logger = logging.getLogger(__name__)


def cmd_fetch():
    from src.ingestion.fetch import fetch_all
    data = fetch_all()
    for name, df in data.items():
        logger.info("  %s: %s rows", name, f"{len(df):,}")


def cmd_clean():
    from src.ingestion.fetch import fetch_all
    from src.ingestion.clean import get_clean_data
    raw = fetch_all()  # Uses cache if available
    clean = get_clean_data(raw)
    for name, df in clean.items():
        logger.info("  %s: %s rows (cleaned)", name, f"{len(df):,}")


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

    logger.info("=== SF Permitting Bottleneck Analysis ===")
    housing = df[df["is_housing"] == True] if "is_housing" in df.columns else df
    logger.info("Total housing permits: %s", f"{len(housing):,}")
    logger.info("Median days filed→issued: %.0f", housing['days_filed_to_issued'].median())
    logger.info("Stuck permits (>1yr): %s", f"{len(stuck):,}")
    if "proposed_units" in stuck.columns:
        logger.info("Housing units blocked: %s", f"{stuck['proposed_units'].sum():,.0f}")

    for name, result in results.items():
        if hasattr(result, 'shape'):
            logger.info("--- %s (%d rows) ---", name, result.shape[0])
            logger.info("\n%s", result.head(10).to_string())


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
    logger.info("Starting dashboard at http://127.0.0.1:8050")
    logger.info("Press Ctrl+C to stop.")
    app.run()


def cmd_pipeline():
    logger.info("Step 1/4: Fetching data from DataSF...")
    cmd_fetch()
    logger.info("Step 2/4: Cleaning data...")
    cmd_clean()
    logger.info("Step 3/4: Generating report...")
    cmd_report()
    logger.info("Step 4/4: Launching dashboard...")
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
    from src.config import setup_logging
    setup_logging()

    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        logger.error("Usage: uv run python main.py {fetch|clean|analyze|report|dashboard|pipeline}")
        sys.exit(1)
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
