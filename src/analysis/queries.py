"""DuckDB query engine for the SF Permitting Bottleneck Analyzer.

All heavy computation happens here via SQL on parquet files.
Typical query time: 20-150ms on 1.3M rows.
"""
import logging
from pathlib import Path

import duckdb
import pandas as pd

from src.config import PROCESSED_DIR, POLICY_MILESTONES

logger = logging.getLogger(__name__)

_PARQUET = str(PROCESSED_DIR / "building_permits.parquet")


def _con() -> duckdb.DuckDBPyConnection:
    """Return a fresh DuckDB connection (in-process, no persistence)."""
    return duckdb.connect()


def _where_clause(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> str:
    """Build a WHERE clause from filter parameters. Always returns 'WHERE ...'."""
    clauses = ["true"]
    if housing_only:
        clauses.append("is_housing = true")
    if districts:
        quoted = ", ".join(f"'{d}'" for d in districts)
        clauses.append(f"supervisor_district IN ({quoted})")
    if year_min is not None:
        clauses.append(f"filed_year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"filed_year <= {int(year_max)}")
    return "WHERE " + " AND ".join(clauses)


def kpis(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> dict:
    """Top-level KPI numbers."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    row = con.sql(f"""
        SELECT
            count(*) as total_permits,
            median(days_filed_to_issued) as median_days_to_issue,
            sum(proposed_units) as total_units_proposed
        FROM '{_PARQUET}' {w}
    """).fetchone()

    # Stuck permits (always housing, always >1yr)
    stuck = con.sql(f"""
        SELECT count(*) as stuck_count, coalesce(sum(proposed_units), 0) as stuck_units
        FROM '{_PARQUET}' {w}
          AND status IN ('Filed', 'Approved', 'filed', 'approved')
          AND filed_date < current_date - INTERVAL '1 year'
          AND issued_date IS NULL
    """).fetchone()

    return {
        "total_permits": row[0],
        "median_days_to_issue": round(row[1], 1) if row[1] is not None else None,
        "total_units_proposed": row[2],
        "stuck_count": stuck[0],
        "stuck_units": int(stuck[1]),
    }


def stage_durations(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> pd.DataFrame:
    """Median/mean/p90 for each pipeline stage."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    return con.sql(f"""
        SELECT
            'Filed → Approved' as stage,
            median(days_filed_to_approved) as median_days,
            avg(days_filed_to_approved) as mean_days,
            quantile_cont(days_filed_to_approved, 0.9) as p90_days,
            count(days_filed_to_approved) as n
        FROM '{_PARQUET}' {w}
        UNION ALL
        SELECT 'Approved → Issued',
            median(days_approved_to_issued), avg(days_approved_to_issued),
            quantile_cont(days_approved_to_issued, 0.9), count(days_approved_to_issued)
        FROM '{_PARQUET}' {w}
        UNION ALL
        SELECT 'Filed → Issued',
            median(days_filed_to_issued), avg(days_filed_to_issued),
            quantile_cont(days_filed_to_issued, 0.9), count(days_filed_to_issued)
        FROM '{_PARQUET}' {w}
        UNION ALL
        SELECT 'Issued → Completed',
            median(days_issued_to_completed), avg(days_issued_to_completed),
            quantile_cont(days_issued_to_completed, 0.9), count(days_issued_to_completed)
        FROM '{_PARQUET}' {w}
    """).fetchdf()


def by_district(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> pd.DataFrame:
    """Per-district stats: median days, permit count, units proposed/stuck."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    return con.sql(f"""
        SELECT
            supervisor_district as district,
            median(days_filed_to_issued) as median_days,
            count(*) as permits,
            sum(proposed_units) as units_proposed
        FROM '{_PARQUET}' {w} AND supervisor_district IS NOT NULL
        GROUP BY supervisor_district
        ORDER BY median_days DESC
    """).fetchdf()


def by_permit_type(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
    top_n: int = 15,
) -> pd.DataFrame:
    """Slowest permit types by median days to issuance."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    return con.sql(f"""
        SELECT
            permit_type_definition as permit_type,
            median(days_filed_to_issued) as median_days,
            count(*) as permits
        FROM '{_PARQUET}' {w} AND permit_type_definition IS NOT NULL
        GROUP BY permit_type_definition
        HAVING count(*) >= 10
        ORDER BY median_days DESC
        LIMIT {int(top_n)}
    """).fetchdf()


def quarterly_trends(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> pd.DataFrame:
    """Quarterly median days_filed_to_issued with rolling average."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    df = con.sql(f"""
        SELECT
            filed_year as year,
            filed_quarter as quarter,
            filed_year || '-Q' || filed_quarter as period,
            median(days_filed_to_issued) as median_days,
            count(*) as permits,
            sum(proposed_units) as units
        FROM '{_PARQUET}' {w} AND filed_year IS NOT NULL AND filed_quarter IS NOT NULL
        GROUP BY filed_year, filed_quarter
        ORDER BY filed_year, filed_quarter
    """).fetchdf()
    if not df.empty:
        df["rolling_avg"] = df["median_days"].rolling(4, min_periods=1).mean()
    return df


def annual_volume(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> pd.DataFrame:
    """Annual permit volume: filed, issued, completed."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    return con.sql(f"""
        SELECT
            filed_year as year,
            count(*) as filed,
            count(issued_date) as issued,
            count(completed_date) as completed,
            sum(proposed_units) as units
        FROM '{_PARQUET}' {w} AND filed_year IS NOT NULL
        GROUP BY filed_year
        ORDER BY filed_year
    """).fetchdf()


def stuck_permits_list(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    threshold_days: int = 365,
    limit: int = 100,
) -> pd.DataFrame:
    """Permits stuck in the pipeline, sorted by wait time."""
    extra_clauses = [
        "is_housing = true",
        "status IN ('Filed', 'Approved', 'filed', 'approved')",
        "issued_date IS NULL",
        f"filed_date < current_date - INTERVAL '{int(threshold_days)} days'",
    ]
    if districts:
        quoted = ", ".join(f"'{d}'" for d in districts)
        extra_clauses.append(f"supervisor_district IN ({quoted})")
    if year_min is not None:
        extra_clauses.append(f"filed_year >= {int(year_min)}")
    if year_max is not None:
        extra_clauses.append(f"filed_year <= {int(year_max)}")

    where = "WHERE " + " AND ".join(extra_clauses)
    con = _con()
    return con.sql(f"""
        SELECT
            permit_number,
            filed_date,
            status,
            current_date - filed_date::date as days_waiting,
            supervisor_district as district,
            neighborhoods_analysis_boundaries as neighborhood,
            proposed_units as units,
            left(description, 120) as description
        FROM '{_PARQUET}' {where}
        ORDER BY days_waiting DESC
        LIMIT {int(limit)}
    """).fetchdf()


def stuck_by_district(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
) -> pd.DataFrame:
    """Stuck permits aggregated by district."""
    extra_clauses = [
        "is_housing = true",
        "status IN ('Filed', 'Approved', 'filed', 'approved')",
        "issued_date IS NULL",
        "filed_date < current_date - INTERVAL '1 year'",
        "supervisor_district IS NOT NULL",
    ]
    if districts:
        quoted = ", ".join(f"'{d}'" for d in districts)
        extra_clauses.append(f"supervisor_district IN ({quoted})")
    if year_min is not None:
        extra_clauses.append(f"filed_year >= {int(year_min)}")
    if year_max is not None:
        extra_clauses.append(f"filed_year <= {int(year_max)}")

    where = "WHERE " + " AND ".join(extra_clauses)
    con = _con()
    return con.sql(f"""
        SELECT
            supervisor_district as district,
            count(*) as stuck_permits,
            coalesce(sum(proposed_units), 0) as stuck_units
        FROM '{_PARQUET}' {where}
        GROUP BY supervisor_district
        ORDER BY stuck_units DESC
    """).fetchdf()


def policy_impact() -> list[dict]:
    """Before/after analysis for each policy milestone."""
    con = _con()
    results = []
    for date_str, event in POLICY_MILESTONES.items():
        row = con.sql(f"""
            SELECT
                median(CASE WHEN filed_date >= '{date_str}'::date - INTERVAL '1 year'
                            AND filed_date < '{date_str}'::date
                       THEN days_filed_to_issued END) as median_before,
                median(CASE WHEN filed_date >= '{date_str}'::date
                            AND filed_date < '{date_str}'::date + INTERVAL '1 year'
                       THEN days_filed_to_issued END) as median_after,
                count(CASE WHEN filed_date >= '{date_str}'::date - INTERVAL '1 year'
                           AND filed_date < '{date_str}'::date
                      THEN 1 END) as permits_before,
                count(CASE WHEN filed_date >= '{date_str}'::date
                           AND filed_date < '{date_str}'::date + INTERVAL '1 year'
                      THEN 1 END) as permits_after
            FROM '{_PARQUET}'
            WHERE is_housing = true
        """).fetchone()
        pct = None
        if row[0] and row[1] and row[0] != 0:
            pct = round(((row[1] - row[0]) / row[0]) * 100, 1)
        results.append({
            "date": date_str,
            "event": event,
            "median_before": round(row[0], 1) if row[0] else None,
            "median_after": round(row[1], 1) if row[1] else None,
            "pct_change": pct,
            "permits_before": row[2],
            "permits_after": row[3],
        })
    return results


def status_breakdown(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    housing_only: bool = True,
) -> pd.DataFrame:
    """Permit counts by status."""
    w = _where_clause(districts, year_min, year_max, housing_only)
    con = _con()
    return con.sql(f"""
        SELECT
            status,
            count(*) as count,
            round(count(*) * 100.0 / sum(count(*)) OVER (), 1) as pct
        FROM '{_PARQUET}' {w}
        GROUP BY status
        ORDER BY count DESC
    """).fetchdf()


def filter_options() -> dict:
    """Get available filter values for the UI."""
    con = _con()
    districts = con.sql(f"""
        SELECT DISTINCT supervisor_district
        FROM '{_PARQUET}'
        WHERE supervisor_district IS NOT NULL
        ORDER BY supervisor_district
    """).fetchdf()["supervisor_district"].tolist()

    years = con.sql(f"""
        SELECT min(filed_year) as min_year, max(filed_year) as max_year
        FROM '{_PARQUET}'
        WHERE filed_year IS NOT NULL
    """).fetchone()

    return {
        "districts": districts,
        "min_year": int(years[0]) if years[0] else 2000,
        "max_year": int(years[1]) if years[1] else 2026,
    }
