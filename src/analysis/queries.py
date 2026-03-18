"""DuckDB query engine for the SF Permitting Bottleneck Analyzer.

All heavy computation happens here via SQL on parquet files.
Typical query time: 20-150ms on 1.3M rows.
"""
import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

from src.config import PROCESSED_DIR, POLICY_MILESTONES, SUPERVISORS, COUNTERFACTUAL

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
        # net_new_units > 0 captures actual housing production, not minor alterations
        clauses.append("net_new_units > 0")
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
            'District ' || cast(supervisor_district as int) as district,
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
        "net_new_units > 0",
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
            'District ' || cast(supervisor_district as int) as district,
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
        "net_new_units > 0",
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
            'District ' || cast(supervisor_district as int) as district,
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
            WHERE net_new_units > 0
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
        SELECT DISTINCT cast(supervisor_district as int) as d
        FROM '{_PARQUET}'
        WHERE supervisor_district IS NOT NULL
        ORDER BY d
    """).fetchdf()["d"].tolist()

    years = con.sql(f"""
        SELECT min(filed_year) as min_year, max(filed_year) as max_year
        FROM '{_PARQUET}'
        WHERE filed_year IS NOT NULL AND filed_year >= 1980
    """).fetchone()

    return {
        "districts": [str(d) for d in districts],
        "min_year": int(years[0]) if years[0] else 1980,
        "max_year": int(years[1]) if years[1] else 2026,
    }


# ---------------------------------------------------------------------------
# Accountability & Counterfactual queries
# ---------------------------------------------------------------------------

_CF = COUNTERFACTUAL  # shorthand


def _stuck_where(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
) -> str:
    """WHERE clause for stuck housing permits (>1yr, not issued)."""
    clauses = [
        "net_new_units > 0",
        "status IN ('Filed', 'Approved', 'filed', 'approved')",
        "issued_date IS NULL",
        "filed_date < current_date - INTERVAL '1 year'",
    ]
    if districts:
        quoted = ", ".join(f"'{d}'" for d in districts)
        clauses.append(f"supervisor_district IN ({quoted})")
    if year_min is not None:
        clauses.append(f"filed_year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"filed_year <= {int(year_max)}")
    return "WHERE " + " AND ".join(clauses)


def counterfactual_impact(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
) -> dict:
    """Citywide counterfactual: what stuck permits are costing SF."""
    w = _stuck_where(districts, year_min, year_max)
    con = _con()
    row = con.sql(f"""
        SELECT
            count(*) as stuck_permits,
            coalesce(sum(proposed_units), 0) as stuck_units,
            coalesce(sum(proposed_units), 0) * {_CF['household_size']} as people_without_homes,
            coalesce(sum(
                proposed_units * {_CF['median_rent_1br_monthly']}
                * (current_date - filed_date::date) / 30.0
            ), 0) as rent_revenue_lost,
            coalesce(sum(
                proposed_units * {_CF['avg_assessed_value_per_unit']}
                * {_CF['property_tax_rate']}
                * (current_date - filed_date::date) / 365.0
            ), 0) as property_tax_lost,
            coalesce(sum(proposed_units), 0) * {_CF['jobs_per_unit']} as jobs_not_created,
            avg(current_date - filed_date::date) as avg_days_waiting
        FROM '{_PARQUET}' {w}
    """).fetchone()
    return {
        "stuck_permits": row[0],
        "stuck_units": int(row[1]),
        "people_without_homes": round(row[2]),
        "rent_revenue_lost": round(row[3]),
        "property_tax_lost": round(row[4]),
        "jobs_not_created": round(row[5]),
        "avg_days_waiting": round(row[6]) if row[6] else 0,
    }


def counterfactual_by_district(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Counterfactual impact broken down by supervisor district."""
    w = _stuck_where(districts, year_min, year_max)
    con = _con()
    df = con.sql(f"""
        SELECT
            cast(supervisor_district as int) as d,
            count(*) as stuck_permits,
            coalesce(sum(proposed_units), 0) as stuck_units,
            coalesce(sum(proposed_units), 0) * {_CF['household_size']} as people_without_homes,
            coalesce(sum(
                proposed_units * {_CF['median_rent_1br_monthly']}
                * (current_date - filed_date::date) / 30.0
            ), 0) as rent_revenue_lost,
            coalesce(sum(
                proposed_units * {_CF['avg_assessed_value_per_unit']}
                * {_CF['property_tax_rate']}
                * (current_date - filed_date::date) / 365.0
            ), 0) as property_tax_lost,
            coalesce(sum(proposed_units), 0) * {_CF['jobs_per_unit']} as jobs_not_created
        FROM '{_PARQUET}' {w}
            AND supervisor_district IS NOT NULL
        GROUP BY supervisor_district
        ORDER BY stuck_units DESC
    """).fetchdf()
    records = df.to_dict(orient="records")
    for r in records:
        r["district"] = f"District {r['d']}"
        info = SUPERVISORS.get(str(r["d"]), {})
        r["supervisor"] = info.get("name", "Unknown") if isinstance(info, dict) else info
        r["email"] = info.get("email", "") if isinstance(info, dict) else ""
    return records


def nimby_signals(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Per-district obstruction signals: disapproval rates, excess delays."""
    clauses = ["net_new_units > 0", "supervisor_district IS NOT NULL"]
    if districts:
        quoted = ", ".join(f"'{d}'" for d in districts)
        clauses.append(f"supervisor_district IN ({quoted})")
    if year_min is not None:
        clauses.append(f"filed_year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"filed_year <= {int(year_max)}")
    w = "WHERE " + " AND ".join(clauses)

    con = _con()
    df = con.sql(f"""
        WITH district_stats AS (
            SELECT
                cast(supervisor_district as int) as d,
                count(*) as total_permits,
                count(CASE WHEN status IN ('Disapproved', 'disapproved') THEN 1 END) as disapproved,
                count(CASE WHEN status IN ('Withdrawn', 'withdrawn') THEN 1 END) as withdrawn,
                count(CASE WHEN lower(description) LIKE '%discretionary review%'
                             OR lower(description) LIKE '%conditional use%' THEN 1 END) as discretionary_mentions,
                median(days_filed_to_issued) as median_days_to_issue,
                median(days_filed_to_approved) as median_days_to_approve
            FROM '{_PARQUET}' {w}
            GROUP BY supervisor_district
        )
        SELECT
            'District ' || d as district,
            total_permits,
            round(disapproved * 100.0 / total_permits, 2) as disapproval_rate_pct,
            round(withdrawn * 100.0 / total_permits, 2) as withdrawal_rate_pct,
            discretionary_mentions,
            round(median_days_to_issue, 1) as median_days_to_issue,
            round(median_days_to_approve, 1) as median_days_to_approve,
            round(median_days_to_approve - (
                SELECT median(median_days_to_approve) FROM district_stats
            ), 1) as excess_approval_delay
        FROM district_stats
        ORDER BY excess_approval_delay DESC NULLS LAST
    """).fetchdf()
    return df.to_dict(orient="records")


def supervisor_scorecard(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Combined district scorecard with supervisor names and impact."""
    clauses = ["net_new_units > 0", "supervisor_district IS NOT NULL"]
    if year_min is not None:
        clauses.append(f"filed_year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"filed_year <= {int(year_max)}")
    w = "WHERE " + " AND ".join(clauses)

    con = _con()
    df = con.sql(f"""
        SELECT
            cast(supervisor_district as int) as d,
            count(*) as total_permits,
            round(median(days_filed_to_issued), 1) as median_days,
            count(CASE WHEN status IN ('Filed','Approved','filed','approved')
                        AND issued_date IS NULL
                        AND filed_date < current_date - INTERVAL '1 year' THEN 1 END) as stuck_permits,
            coalesce(sum(CASE WHEN status IN ('Filed','Approved','filed','approved')
                        AND issued_date IS NULL
                        AND filed_date < current_date - INTERVAL '1 year'
                        THEN proposed_units END), 0) as stuck_units,
            coalesce(sum(CASE WHEN status IN ('Filed','Approved','filed','approved')
                        AND issued_date IS NULL
                        AND filed_date < current_date - INTERVAL '1 year'
                        THEN proposed_units END), 0) * {_CF['household_size']} as people_without_homes,
            coalesce(sum(CASE WHEN status IN ('Filed','Approved','filed','approved')
                        AND issued_date IS NULL
                        AND filed_date < current_date - INTERVAL '1 year'
                        THEN proposed_units * {_CF['avg_assessed_value_per_unit']}
                             * {_CF['property_tax_rate']}
                             * (current_date - filed_date::date) / 365.0 END), 0) as property_tax_lost
        FROM '{_PARQUET}' {w}
        GROUP BY supervisor_district
        ORDER BY stuck_units DESC
    """).fetchdf()
    records = df.to_dict(orient="records")
    for r in records:
        info = SUPERVISORS.get(str(r["d"]), {})
        r["supervisor"] = info.get("name", "Unknown") if isinstance(info, dict) else info
        r["email"] = info.get("email", "") if isinstance(info, dict) else ""
    return records


def worst_stuck_narratives(
    districts: list[str] | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 10,
) -> list[dict]:
    """Top stuck permits ranked by impact (units * days), with narrative data."""
    w = _stuck_where(districts, year_min, year_max)
    con = _con()
    df = con.sql(f"""
        SELECT
            permit_number,
            filed_date,
            status,
            current_date - filed_date::date as days_waiting,
            round((current_date - filed_date::date) / 365.0, 1) as years_waiting,
            'District ' || cast(supervisor_district as int) as district,
            neighborhoods_analysis_boundaries as neighborhood,
            proposed_units as units,
            round(proposed_units * {_CF['household_size']}, 0) as people_affected,
            round(proposed_units * {_CF['median_rent_1br_monthly']}
                  * (current_date - filed_date::date) / 30.0, 0) as rent_revenue_lost,
            coalesce(street_number, '') || ' ' || coalesce(street_name, '') || ' '
                || coalesce(street_suffix, '') as address,
            description
        FROM '{_PARQUET}' {w}
            AND supervisor_district IS NOT NULL
        ORDER BY proposed_units * (current_date - filed_date::date) DESC
        LIMIT {int(limit)}
    """).fetchdf()
    return json.loads(df.to_json(orient="records", date_format="iso"))
