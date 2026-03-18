"""Trend analysis module for SF permitting data.

Provides quarterly, annual, seasonal, policy-impact, and district-level
trend analyses on housing permit processing durations.
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.config import POLICY_MILESTONES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DURATION_COLUMNS = [
    "days_filed_to_approved",
    "days_approved_to_issued",
    "days_filed_to_issued",
    "days_issued_to_completed",
    "days_total",
]


def _housing_only(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to housing permits only."""
    return df[df["is_housing"] == True].copy()  # noqa: E712


def _period_label(year: int, quarter: int) -> str:
    """Return a human-readable period label like '2023-Q1'."""
    return f"{year}-Q{quarter}"


# ---------------------------------------------------------------------------
# 1. Quarterly trends
# ---------------------------------------------------------------------------


def quarterly_trends(
    df: pd.DataFrame,
    metric: str = "days_filed_to_issued",
) -> pd.DataFrame:
    """Quarterly median of *metric* for housing permits.

    Returns a DataFrame with columns:
        year, quarter, period, median_{metric}, count,
        total_units_proposed, rolling_avg
    """
    housing = _housing_only(df)

    grouped = housing.groupby(["filed_year", "filed_quarter"]).agg(
        median_value=(metric, "median"),
        count=(metric, "count"),
        total_units_proposed=("proposed_units", "sum"),
    ).reset_index()

    grouped = grouped.rename(columns={
        "filed_year": "year",
        "filed_quarter": "quarter",
    })

    grouped["period"] = grouped.apply(
        lambda r: _period_label(int(r["year"]), int(r["quarter"])), axis=1
    )

    # Rename the median column to include the metric name
    median_col = f"median_{metric}"
    grouped = grouped.rename(columns={"median_value": median_col})

    # Sort chronologically before computing rolling average
    grouped = grouped.sort_values(["year", "quarter"]).reset_index(drop=True)

    # 4-quarter rolling average of the median
    grouped["rolling_avg"] = grouped[median_col].rolling(window=4, min_periods=1).mean()

    # Reorder columns
    grouped = grouped[["year", "quarter", "period", median_col, "count",
                        "total_units_proposed", "rolling_avg"]]

    return grouped


# ---------------------------------------------------------------------------
# 2. Annual trends
# ---------------------------------------------------------------------------


def annual_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Annual summary with median durations for every stage, permit counts,
    unit counts, and year-over-year percent change for each numeric column.
    """
    housing = _housing_only(df)

    agg_dict: dict[str, tuple[str, str]] = {}
    for col in _DURATION_COLUMNS:
        agg_dict[f"median_{col}"] = (col, "median")

    agg_dict["permit_count"] = ("days_filed_to_issued", "count")
    agg_dict["total_units_proposed"] = ("proposed_units", "sum")
    agg_dict["total_net_new_units"] = ("net_new_units", "sum")

    grouped = (
        housing.groupby("filed_year")
        .agg(**agg_dict)
        .reset_index()
        .rename(columns={"filed_year": "year"})
        .sort_values("year")
        .reset_index(drop=True)
    )

    # Year-over-year percent change for every numeric column
    numeric_cols = [c for c in grouped.columns if c != "year"]
    for col in numeric_cols:
        grouped[f"{col}_yoy_pct"] = grouped[col].pct_change() * 100

    return grouped


# ---------------------------------------------------------------------------
# 3. Seasonal patterns
# ---------------------------------------------------------------------------


def seasonal_patterns(
    df: pd.DataFrame,
    metric: str = "days_filed_to_issued",
) -> pd.DataFrame:
    """Average *metric* by quarter-of-year (Q1-Q4) across all years.

    Returns columns: quarter, label, mean_{metric}, median_{metric}, count
    """
    housing = _housing_only(df)

    grouped = (
        housing.groupby("filed_quarter")
        .agg(
            mean_value=(metric, "mean"),
            median_value=(metric, "median"),
            count=(metric, "count"),
        )
        .reset_index()
        .rename(columns={
            "filed_quarter": "quarter",
            "mean_value": f"mean_{metric}",
            "median_value": f"median_{metric}",
        })
        .sort_values("quarter")
        .reset_index(drop=True)
    )

    grouped["label"] = grouped["quarter"].apply(lambda q: f"Q{int(q)}")

    # Reorder
    grouped = grouped[["quarter", "label", f"mean_{metric}",
                        f"median_{metric}", "count"]]

    return grouped


# ---------------------------------------------------------------------------
# 4. Policy impact analysis
# ---------------------------------------------------------------------------


def policy_impact_analysis(df: pd.DataFrame) -> list[dict]:
    """For each POLICY_MILESTONES date, compare median days_filed_to_issued
    in the 12 months before vs 12 months after the policy date.

    Returns a list of dicts, each with:
        date, event, median_before, median_after, pct_change,
        permits_before, permits_after
    """
    housing = _housing_only(df).copy()

    # Ensure filed_date is datetime
    if not pd.api.types.is_datetime64_any_dtype(housing["filed_date"]):
        housing["filed_date"] = pd.to_datetime(housing["filed_date"])

    metric = "days_filed_to_issued"
    results: list[dict] = []

    for date_str, event in POLICY_MILESTONES.items():
        policy_date = pd.Timestamp(date_str)
        before_start = policy_date - timedelta(days=365)
        after_end = policy_date + timedelta(days=365)

        before_mask = (housing["filed_date"] >= before_start) & (housing["filed_date"] < policy_date)
        after_mask = (housing["filed_date"] >= policy_date) & (housing["filed_date"] < after_end)

        before_df = housing.loc[before_mask, metric].dropna()
        after_df = housing.loc[after_mask, metric].dropna()

        median_before = float(before_df.median()) if len(before_df) > 0 else None
        median_after = float(after_df.median()) if len(after_df) > 0 else None

        if median_before is not None and median_after is not None and median_before != 0:
            pct_change = ((median_after - median_before) / median_before) * 100
        else:
            pct_change = None

        results.append({
            "date": date_str,
            "event": event,
            "median_before": median_before,
            "median_after": median_after,
            "pct_change": pct_change,
            "permits_before": int(len(before_df)),
            "permits_after": int(len(after_df)),
        })

    return results


# ---------------------------------------------------------------------------
# 5. District trend
# ---------------------------------------------------------------------------


def district_trend(
    df: pd.DataFrame,
    district: str,
    metric: str = "days_filed_to_issued",
) -> pd.DataFrame:
    """Quarterly trend of *metric* for a specific supervisor district.

    Returns the same shape as quarterly_trends but filtered to one district.
    """
    housing = _housing_only(df)
    district_df = housing[housing["supervisor_district"] == district].copy()

    if district_df.empty:
        return pd.DataFrame(columns=[
            "district", "year", "quarter", "period",
            f"median_{metric}", "count", "total_units_proposed", "rolling_avg",
        ])

    grouped = district_df.groupby(["filed_year", "filed_quarter"]).agg(
        median_value=(metric, "median"),
        count=(metric, "count"),
        total_units_proposed=("proposed_units", "sum"),
    ).reset_index()

    grouped = grouped.rename(columns={
        "filed_year": "year",
        "filed_quarter": "quarter",
    })

    grouped["period"] = grouped.apply(
        lambda r: _period_label(int(r["year"]), int(r["quarter"])), axis=1
    )

    median_col = f"median_{metric}"
    grouped = grouped.rename(columns={"median_value": median_col})

    grouped = grouped.sort_values(["year", "quarter"]).reset_index(drop=True)

    grouped["rolling_avg"] = grouped[median_col].rolling(window=4, min_periods=1).mean()
    grouped["district"] = district

    grouped = grouped[["district", "year", "quarter", "period", median_col,
                        "count", "total_units_proposed", "rolling_avg"]]

    return grouped


# ---------------------------------------------------------------------------
# 6. Get all trends
# ---------------------------------------------------------------------------


def get_all_trends(df: pd.DataFrame) -> dict:
    """Run all trend analyses and return results as a dict.

    Keys:
        quarterly, annual, seasonal, policy_impact,
        district_trends (dict of district -> DataFrame)
    """
    results: dict = {}

    results["quarterly"] = quarterly_trends(df)
    results["annual"] = annual_trends(df)
    results["seasonal"] = seasonal_patterns(df)
    results["policy_impact"] = policy_impact_analysis(df)

    # Per-district trends for each unique district present in the data
    housing = _housing_only(df)
    districts = sorted(housing["supervisor_district"].dropna().unique())
    district_results: dict[str, pd.DataFrame] = {}
    for dist in districts:
        district_results[str(dist)] = district_trend(df, dist)
    results["district_trends"] = district_results

    return results
