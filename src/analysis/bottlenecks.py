"""Bottleneck analysis engine for SF permitting data.

Provides functions to identify delays, stuck permits, and district-level
scorecards from the processed building-permits DataFrame.
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from src.config import HOUSING_PERMIT_TYPES

logger = logging.getLogger(__name__)

# Duration columns used across multiple analyses
DURATION_COLUMNS = [
    "days_filed_to_approved",
    "days_approved_to_issued",
    "days_filed_to_issued",
    "days_issued_to_completed",
    "days_total",
]


def _housing_only(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to housing-related permits using the config list."""
    return df.loc[df["is_housing"] == True].copy()


def stage_duration_summary(
    df: pd.DataFrame,
    group_by: Optional[str] = None,
) -> pd.DataFrame:
    """Compute descriptive statistics for each duration column.

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.
    group_by : str or None
        Column to group by before aggregating. If *None*, compute overall stats.

    Returns
    -------
    pd.DataFrame
        One row per duration column (or per group x duration column) with
        median, mean, p25, p75, and p90.
    """
    logger.info("Computing stage duration summary...")
    housing = _housing_only(df)

    agg_funcs = {
        "median": "median",
        "mean": "mean",
        "p25": lambda x: np.nanpercentile(x.dropna(), 25) if len(x.dropna()) > 0 else np.nan,
        "p75": lambda x: np.nanpercentile(x.dropna(), 75) if len(x.dropna()) > 0 else np.nan,
        "p90": lambda x: np.nanpercentile(x.dropna(), 90) if len(x.dropna()) > 0 else np.nan,
    }

    present_cols = [c for c in DURATION_COLUMNS if c in housing.columns]

    if group_by is not None:
        frames = []
        for col in present_cols:
            grouped = housing.groupby(group_by)[col].agg(**agg_funcs).reset_index()  # type: ignore[call-overload]
            grouped.insert(0, "stage", col)
            frames.append(grouped)
        result = pd.concat(frames, ignore_index=True)
    else:
        rows = []
        for col in present_cols:
            series = housing[col].dropna()
            rows.append(
                {
                    "stage": col,
                    "median": series.median() if len(series) > 0 else np.nan,
                    "mean": series.mean() if len(series) > 0 else np.nan,
                    "p25": np.nanpercentile(series, 25) if len(series) > 0 else np.nan,
                    "p75": np.nanpercentile(series, 75) if len(series) > 0 else np.nan,
                    "p90": np.nanpercentile(series, 90) if len(series) > 0 else np.nan,
                }
            )
        result = pd.DataFrame(rows)

    return result  # type: ignore[no-any-return]


def worst_bottlenecks(
    df: pd.DataFrame,
    stage: str = "days_filed_to_issued",
    top_n: int = 10,
    group_by: str = "supervisor_district",
) -> pd.DataFrame:
    """Identify the groups with the longest median duration for a given stage.

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.
    stage : str
        Duration column to rank on.
    top_n : int
        Number of worst groups to return.
    group_by : str
        Column to group by.

    Returns
    -------
    pd.DataFrame
        Top *top_n* groups sorted by median duration descending, with
        median, mean, p75, p90, and count.
    """
    logger.info("Finding worst bottlenecks by %s...", group_by)
    housing = _housing_only(df)

    grouped = housing.groupby(group_by)[stage].agg(
        median="median",
        mean="mean",
        p75=lambda x: np.nanpercentile(x.dropna(), 75) if len(x.dropna()) > 0 else np.nan,
        p90=lambda x: np.nanpercentile(x.dropna(), 90) if len(x.dropna()) > 0 else np.nan,
        count="count",
    ).reset_index()

    grouped = grouped.sort_values("median", ascending=False).head(top_n).reset_index(drop=True)
    return grouped


def permit_status_breakdown(
    df: pd.DataFrame,
    group_by: Optional[str] = None,
) -> pd.DataFrame:
    """Count permits by status, optionally grouped.

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.
    group_by : str or None
        Optional column to further group by (e.g. ``"filed_year"``).

    Returns
    -------
    pd.DataFrame
        Counts and percentages per status (and per group if given).
    """
    logger.info("Computing permit status breakdown...")
    if group_by is not None:
        counts = df.groupby([group_by, "status"]).size().reset_index(name="count")
        totals = counts.groupby(group_by)["count"].transform("sum")
        counts["pct"] = (counts["count"] / totals * 100).round(2)
    else:
        counts = df.groupby("status").size().reset_index(name="count")
        total = counts["count"].sum()
        counts["pct"] = (counts["count"] / total * 100).round(2)

    return counts.sort_values("count", ascending=False).reset_index(drop=True)


def stuck_permits(
    df: pd.DataFrame,
    threshold_days: int = 365,
) -> pd.DataFrame:
    """Find permits that are filed or approved but not yet issued.

    A permit is considered *stuck* when the number of days since filing
    exceeds *threshold_days* and the permit has not yet been issued.

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.
    threshold_days : int
        Minimum days since filing to qualify as stuck.

    Returns
    -------
    pd.DataFrame
        Stuck permits sorted by *days_waiting* descending.
    """
    logger.info("Finding stuck permits (threshold: %d days)...", threshold_days)
    today = pd.Timestamp(datetime.today().date())

    # Permits that are filed or approved but not issued
    not_issued_mask = df["status"].str.lower().isin(["filed", "approved"])
    subset = df.loc[not_issued_mask].copy()

    filed = pd.to_datetime(subset["filed_date"], errors="coerce")
    subset["days_waiting"] = (today - filed).dt.days

    stuck = subset.loc[subset["days_waiting"] > threshold_days].copy()

    output_cols = [
        "permit_number",
        "filed_date",
        "status",
        "days_waiting",
        "supervisor_district",
        "neighborhoods_analysis_boundaries",
        "proposed_units",
    ]
    # Include description if present in the DataFrame
    if "description" in stuck.columns:
        output_cols.append("description")

    available = [c for c in output_cols if c in stuck.columns]
    stuck = stuck[available].sort_values("days_waiting", ascending=False).reset_index(drop=True)
    return stuck


def volume_analysis(
    df: pd.DataFrame,
    group_by: str = "filed_year",
) -> pd.DataFrame:
    """Count permits filed, issued, and completed per group.

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.
    group_by : str
        Column to group by (default ``"filed_year"``).

    Returns
    -------
    pd.DataFrame
        Counts of filed, issued, completed permits and sum of net_new_units
        per group.
    """
    logger.info("Running volume analysis by %s...", group_by)
    filed = df.groupby(group_by).size().reset_index(name="permits_filed")

    issued_mask = df["issued_date"].notna()
    issued = df.loc[issued_mask].groupby(group_by).size().reset_index(name="permits_issued")

    completed_mask = df["completed_date"].notna()
    completed = df.loc[completed_mask].groupby(group_by).size().reset_index(name="permits_completed")

    units = df.groupby(group_by)["net_new_units"].sum().reset_index(name="net_new_units")

    result = (
        filed
        .merge(issued, on=group_by, how="left")
        .merge(completed, on=group_by, how="left")
        .merge(units, on=group_by, how="left")
    )

    for col in ["permits_issued", "permits_completed"]:
        result[col] = result[col].fillna(0).astype(int)

    return result.sort_values(group_by).reset_index(drop=True)


def district_scorecard(df: pd.DataFrame) -> pd.DataFrame:
    """Build a per-district scorecard with a composite bottleneck score.

    Metrics per supervisor district:
    - median_days_to_issuance: median ``days_filed_to_issued``
    - total_permits: count of all permits
    - units_proposed: sum of ``proposed_units``
    - units_stuck: sum of ``proposed_units`` for stuck permits (filed/approved, >365 days)
    - bottleneck_score: rank-based composite (higher = worse)

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per district, sorted by *bottleneck_score* descending.
    """
    logger.info("Building district scorecard...")
    col = "supervisor_district"

    # Median days to issuance (housing only)
    housing = _housing_only(df)
    median_days = (
        housing.groupby(col)["days_filed_to_issued"]
        .median()
        .reset_index(name="median_days_to_issuance")
    )

    # Total permits
    total_permits = df.groupby(col).size().reset_index(name="total_permits")

    # Units proposed
    units_proposed = df.groupby(col)["proposed_units"].sum().reset_index(name="units_proposed")

    # Units stuck: filed/approved but not issued and waiting > 365 days
    stuck = stuck_permits(df, threshold_days=365)
    if "proposed_units" in stuck.columns and len(stuck) > 0:
        units_stuck = (
            stuck.groupby(col)["proposed_units"]
            .sum()
            .reset_index(name="units_stuck")
        )
    else:
        units_stuck = pd.DataFrame({col: pd.Series(dtype="object"), "units_stuck": pd.Series(dtype="float64")})

    scorecard = (
        median_days
        .merge(total_permits, on=col, how="outer")
        .merge(units_proposed, on=col, how="outer")
        .merge(units_stuck, on=col, how="outer")
    )
    scorecard["units_stuck"] = scorecard["units_stuck"].fillna(0)

    # Composite bottleneck score: average of per-metric ranks (higher rank = worse)
    scorecard["rank_days"] = scorecard["median_days_to_issuance"].rank(ascending=True, method="min")
    scorecard["rank_stuck"] = scorecard["units_stuck"].rank(ascending=True, method="min")

    scorecard["bottleneck_score"] = ((scorecard["rank_days"] + scorecard["rank_stuck"]) / 2).round(2)

    scorecard = scorecard.drop(columns=["rank_days", "rank_stuck"])
    scorecard = scorecard.sort_values("bottleneck_score", ascending=False).reset_index(drop=True)
    return scorecard


def get_all_analyses(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Run every analysis and return results keyed by name.

    Parameters
    ----------
    df : pd.DataFrame
        Processed permits DataFrame.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    logger.info("Running all bottleneck analyses...")
    return {
        "stage_duration_summary": stage_duration_summary(df),
        "worst_bottlenecks": worst_bottlenecks(df),
        "permit_status_breakdown": permit_status_breakdown(df),
        "stuck_permits": stuck_permits(df),
        "volume_analysis": volume_analysis(df),
        "district_scorecard": district_scorecard(df),
    }
