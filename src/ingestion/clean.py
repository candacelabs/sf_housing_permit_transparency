"""Data cleaning and normalization for SF permitting datasets.

Takes raw DataFrames from the ingestion layer and produces clean,
analysis-ready data with parsed dates, numeric fields, computed
durations, and standardized categories.
"""

import logging
from pathlib import Path

import pandas as pd

from src.config import (
    HOUSING_PERMIT_TYPES,
    PROCESSED_DIR,
    STAGE_DATE_COLUMNS,
    STATUS_MAPPING,
)

logger = logging.getLogger(__name__)

# Date columns present in the building permits dataset
_BUILDING_PERMIT_DATE_COLUMNS = [
    "filed_date",
    "approved_date",
    "issued_date",
    "first_construction_document_date",
    "completed_date",
    "status_date",
    "permit_creation_date",
]

# Maximum plausible duration in days (~20 years)
_MAX_DURATION_DAYS = 7300.0


def _parse_currency(series: pd.Series) -> pd.Series:
    """Strip $ and commas from a currency column, return as float."""
    return pd.to_numeric(
        series.astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False),
        errors="coerce",
    )


def _duration_days(start: pd.Series, end: pd.Series) -> pd.Series:
    """Compute calendar-day duration between two datetime columns.

    Returns NaN where either date is missing, the duration is negative,
    or the duration exceeds _MAX_DURATION_DAYS.
    """
    delta = (end - start).dt.total_seconds() / 86400.0
    delta = delta.where(delta >= 0)
    delta = delta.where(delta <= _MAX_DURATION_DAYS)
    return delta


def _is_housing_use(series: pd.Series) -> pd.Series:
    """Check if a use column contains housing-related keywords (case insensitive)."""
    lowered = series.astype(str).str.lower()
    return lowered.str.contains("apartments", na=False) | lowered.str.contains(
        "residential", na=False
    )


def clean_building_permits(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize the Building Permits dataset (i98e-djp9).

    Parameters
    ----------
    df : pd.DataFrame
        Raw building permits data from Socrata.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with parsed dates, numeric costs/units,
        computed durations, and classification flags.
    """
    df = df.copy()
    logger.info("Cleaning building permits: %d rows", len(df))

    # -- Normalize column names to snake_case --------------------------------
    # CSV exports use title case ("Permit Type"), Socrata API uses snake_case
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    # Handle known column name differences between CSV and API
    col_renames = {
        "current_status": "status",
        "current_status_date": "status_date",
    }
    df = df.rename(columns={k: v for k, v in col_renames.items() if k in df.columns})

    # -- Parse date columns --------------------------------------------------
    for col in _BUILDING_PERMIT_DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # -- Parse cost columns ---------------------------------------------------
    if "estimated_cost" in df.columns:
        df["estimated_cost"] = _parse_currency(df["estimated_cost"])
    if "revised_cost" in df.columns:
        df["revised_cost"] = _parse_currency(df["revised_cost"])

    # -- Parse unit columns ---------------------------------------------------
    if "existing_units" in df.columns:
        df["existing_units"] = pd.to_numeric(df["existing_units"], errors="coerce")
    if "proposed_units" in df.columns:
        df["proposed_units"] = pd.to_numeric(df["proposed_units"], errors="coerce")

    # -- Compute net new units ------------------------------------------------
    if "existing_units" in df.columns and "proposed_units" in df.columns:
        df["net_new_units"] = df["proposed_units"] - df["existing_units"]

    # -- Parse permit_type to numeric -----------------------------------------
    if "permit_type" in df.columns:
        df["permit_type"] = pd.to_numeric(df["permit_type"], errors="coerce")

    # -- Normalize status using STATUS_MAPPING --------------------------------
    if "status" in df.columns:
        lowered = df["status"].astype(str).str.strip().str.lower()
        df["status"] = lowered.map(STATUS_MAPPING).fillna(df["status"])

    # -- Stage duration columns (calendar days) -------------------------------
    if "filed_date" in df.columns and "approved_date" in df.columns:
        df["days_filed_to_approved"] = _duration_days(df["filed_date"], df["approved_date"])
    if "approved_date" in df.columns and "issued_date" in df.columns:
        df["days_approved_to_issued"] = _duration_days(df["approved_date"], df["issued_date"])
    if "filed_date" in df.columns and "issued_date" in df.columns:
        df["days_filed_to_issued"] = _duration_days(df["filed_date"], df["issued_date"])
    if "issued_date" in df.columns and "first_construction_document_date" in df.columns:
        df["days_issued_to_first_construction"] = _duration_days(
            df["issued_date"], df["first_construction_document_date"]
        )
    if "issued_date" in df.columns and "completed_date" in df.columns:
        df["days_issued_to_completed"] = _duration_days(df["issued_date"], df["completed_date"])
    if "filed_date" in df.columns and "completed_date" in df.columns:
        df["days_total"] = _duration_days(df["filed_date"], df["completed_date"])

    # -- Filed year and quarter -----------------------------------------------
    if "filed_date" in df.columns:
        df["filed_year"] = df["filed_date"].dt.year.astype("Int64")
        df["filed_quarter"] = df["filed_date"].dt.quarter.astype("Int64")

    # -- Housing flag ---------------------------------------------------------
    permit_type_match = pd.Series(False, index=df.index)
    if "permit_type" in df.columns:
        permit_type_match = df["permit_type"].isin(HOUSING_PERMIT_TYPES)

    units_match = pd.Series(False, index=df.index)
    if "proposed_units" in df.columns:
        units_match = df["proposed_units"].fillna(0) > 0

    use_match = pd.Series(False, index=df.index)
    if "existing_use" in df.columns:
        use_match = use_match | _is_housing_use(df["existing_use"])
    if "proposed_use" in df.columns:
        use_match = use_match | _is_housing_use(df["proposed_use"])

    df["is_housing"] = permit_type_match | units_match | use_match

    logger.info(
        "Building permits cleaned: %d rows, %d housing-related",
        len(df),
        df["is_housing"].sum(),
    )
    return df


def clean_affordable_housing(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize the Affordable Housing Pipeline dataset (aaxw-2cb8).

    Parameters
    ----------
    df : pd.DataFrame
        Raw affordable housing data from Socrata.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with parsed dates, numeric units, and
        standardized column names.
    """
    df = df.copy()
    logger.info("Cleaning affordable housing: %d rows", len(df))

    # -- Standardize column names (lowercase, underscores) --------------------
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)

    # -- Parse date columns ---------------------------------------------------
    date_candidates = [c for c in df.columns if "date" in c.lower()]
    for col in date_candidates:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # -- Parse unit count columns ---------------------------------------------
    unit_candidates = [c for c in df.columns if "unit" in c.lower()]
    for col in unit_candidates:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    logger.info("Affordable housing cleaned: %d rows", len(df))
    return df


def get_clean_data(raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Orchestrate cleaning of all datasets and save as parquet.

    Parameters
    ----------
    raw_data : dict[str, pd.DataFrame]
        Mapping of dataset name to raw DataFrame. Expected keys:
        ``"building_permits"``, ``"affordable_housing"``,
        ``"development_pipeline"`` (optional).

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping of dataset name to cleaned DataFrame.
    """
    clean: dict[str, pd.DataFrame] = {}

    # Ensure output directory exists
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # -- Building permits -----------------------------------------------------
    if "building_permits" in raw_data:
        cleaned_bp = clean_building_permits(raw_data["building_permits"])
        output_path = PROCESSED_DIR / "building_permits.parquet"
        cleaned_bp.to_parquet(output_path, index=False)
        logger.info("Saved building permits to %s", output_path)
        clean["building_permits"] = cleaned_bp

    # -- Affordable housing ---------------------------------------------------
    if "affordable_housing" in raw_data:
        cleaned_ah = clean_affordable_housing(raw_data["affordable_housing"])
        output_path = PROCESSED_DIR / "affordable_housing.parquet"
        cleaned_ah.to_parquet(output_path, index=False)
        logger.info("Saved affordable housing to %s", output_path)
        clean["affordable_housing"] = cleaned_ah

    # -- Development pipeline (pass-through with column standardization) ------
    if "development_pipeline" in raw_data:
        cleaned_dp = raw_data["development_pipeline"].copy()
        cleaned_dp.columns = (
            cleaned_dp.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        )
        # Parse any date columns
        date_candidates = [c for c in cleaned_dp.columns if "date" in c.lower()]
        for col in date_candidates:
            cleaned_dp[col] = pd.to_datetime(cleaned_dp[col], errors="coerce")
        output_path = PROCESSED_DIR / "development_pipeline.parquet"
        cleaned_dp.to_parquet(output_path, index=False)
        logger.info("Saved development pipeline to %s", output_path)
        clean["development_pipeline"] = cleaned_dp

    logger.info("All datasets cleaned: %s", list(clean.keys()))
    return clean
