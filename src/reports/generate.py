"""Generate HTML policy brief from analysis results."""
import pandas as pd
from datetime import datetime
from jinja2 import Environment
from pathlib import Path

from src.config import PROCESSED_DIR, REPORTS_DIR, POLICY_MILESTONES
from src.analysis.bottlenecks import (
    stage_duration_summary, stuck_permits, district_scorecard, volume_analysis,
)
from src.analysis.trends import quarterly_trends, annual_trends, policy_impact_analysis


_jinja_env = Environment()
_jinja_env.filters["comma"] = lambda v: f"{int(v):,}" if v and v == v else str(v)
_jinja_env.filters["round_num"] = lambda v: f"{float(v):,.0f}" if v and v == v else str(v)

REPORT_TEMPLATE = _jinja_env.from_string("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>SF Permitting Bottleneck Report</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, sans-serif; max-width: 900px; margin: 0 auto; padding: 40px 20px; color: #2c3e50; line-height: 1.6; }
        h1 { color: #1a5276; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        h2 { color: #2471a3; margin-top: 40px; }
        .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 20px 0; }
        .kpi { background: #f8f9fa; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #3498db; }
        .kpi.danger { border-left-color: #e74c3c; }
        .kpi.warning { border-left-color: #f39c12; }
        .kpi-value { font-size: 2em; font-weight: 700; color: #2c3e50; }
        .kpi-label { font-size: 0.85em; color: #7f8c8d; margin-top: 4px; }
        table { border-collapse: collapse; width: 100%; margin: 16px 0; }
        th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }
        th { background: #2c3e50; color: white; }
        tr:nth-child(even) { background: #f8f9fa; }
        .callout { background: #fef9e7; border-left: 4px solid #f39c12; padding: 16px; margin: 20px 0; border-radius: 4px; }
        .callout.danger { background: #fdedec; border-left-color: #e74c3c; }
        .footer { margin-top: 60px; padding-top: 20px; border-top: 1px solid #ddd; color: #95a5a6; font-size: 0.85em; }
        .tl-dr { background: #eaf2f8; padding: 20px; border-radius: 8px; margin: 20px 0; }
        .tl-dr h3 { margin-top: 0; color: #1a5276; }
    </style>
</head>
<body>
    <h1>San Francisco Permitting Bottleneck Report</h1>
    <p><em>Generated {{ generated_date }} from DBI Building Permits data ({{ total_permits | comma }} permits analyzed)</em></p>

    <div class="tl-dr">
        <h3>TL;DR for Policymakers</h3>
        <ul>
            <li><strong>{{ stuck_count | comma }} permits</strong> representing <strong>{{ stuck_units | comma }} housing units</strong> are stuck in the pipeline (filed over a year ago, still not issued).</li>
            <li>The median time from filing to permit issuance is <strong>{{ median_days_to_issue | round_num }} days</strong> ({{ (median_days_to_issue / 365) | round_num }} years).</li>
            <li>The slowest district is <strong>District {{ worst_district }}</strong> with a median of {{ worst_district_days | round_num }} days to issuance.</li>
            {% if trend_direction == "worse" %}
            <li>Processing times have <strong>gotten worse</strong> — the recent quarterly median is {{ recent_median | round_num }} days vs {{ historical_median | round_num }} days historically.</li>
            {% else %}
            <li>Processing times have <strong>improved</strong> — the recent quarterly median is {{ recent_median | round_num }} days vs {{ historical_median | round_num }} days historically.</li>
            {% endif %}
        </ul>
    </div>

    <div class="kpi-grid">
        <div class="kpi">
            <div class="kpi-value">{{ total_housing | comma }}</div>
            <div class="kpi-label">Housing Permits Analyzed</div>
        </div>
        <div class="kpi warning">
            <div class="kpi-value">{{ median_days_to_issue | round_num }}</div>
            <div class="kpi-label">Median Days to Issuance</div>
        </div>
        <div class="kpi danger">
            <div class="kpi-value">{{ stuck_count | comma }}</div>
            <div class="kpi-label">Permits Stuck (>1yr)</div>
        </div>
        <div class="kpi danger">
            <div class="kpi-value">{{ stuck_units | comma }}</div>
            <div class="kpi-label">Housing Units Blocked</div>
        </div>
    </div>

    <h2>District Scorecard</h2>
    <p>How each Supervisor District compares on permitting speed and volume.</p>
    {{ district_table }}

    <h2>Where Permits Get Stuck</h2>
    <p>Breakdown of processing time by pipeline stage (median days, housing permits only).</p>
    {{ stage_table }}

    {% if policy_impact %}
    <h2>Did Policy Changes Help?</h2>
    <p>Comparing median processing times 12 months before and after key policy milestones.</p>
    {{ policy_table }}
    {% endif %}

    <h2>Top 20 Longest-Stuck Permits</h2>
    <div class="callout danger">
        These permits have been waiting the longest. Each row represents housing that could exist but doesn't.
    </div>
    {{ stuck_table }}

    <div class="footer">
        <p>Data source: <a href="https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9">SF Open Data — DBI Building Permits</a></p>
        <p>Generated by the SF Permitting Bottleneck Analyzer. For questions, contact the analysis team.</p>
    </div>
</body>
</html>""")


def _comma(value):
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


def _round_num(value):
    try:
        return f"{float(value):,.0f}"
    except (ValueError, TypeError):
        return str(value)


def _df_to_html(df, max_rows=30):
    """Convert DataFrame to simple HTML table."""
    if df is None or df.empty:
        return "<p><em>No data available</em></p>"
    display = df.head(max_rows).copy()
    for col in display.select_dtypes(include=["float64", "float32"]).columns:
        display[col] = display[col].round(1)
    for col in display.select_dtypes(include=["datetime64"]).columns:
        display[col] = display[col].dt.strftime("%Y-%m-%d")
    return display.to_html(index=False, classes="report-table", na_rep="-")


def generate_report(df: pd.DataFrame) -> Path:
    """Generate HTML policy brief and save to reports/ directory."""
    housing = df[df["is_housing"] == True] if "is_housing" in df.columns else df
    stuck = stuck_permits(df)
    scorecard = district_scorecard(df)

    # Compute context variables
    median_days = housing["days_filed_to_issued"].median() if "days_filed_to_issued" in housing.columns else 0
    stuck_units = stuck["proposed_units"].sum() if not stuck.empty and "proposed_units" in stuck.columns else 0

    # Worst district
    if not scorecard.empty and "median_days_to_issuance" in scorecard.columns:
        worst_row = scorecard.loc[scorecard["median_days_to_issuance"].idxmax()]
        worst_district = worst_row["supervisor_district"]
        worst_district_days = worst_row["median_days_to_issuance"]
    else:
        worst_district = "N/A"
        worst_district_days = 0

    # Trend direction
    qt = quarterly_trends(df)
    if not qt.empty:
        metric_col = [c for c in qt.columns if c.startswith("median_")][0] if any(c.startswith("median_") for c in qt.columns) else None
        if metric_col:
            recent = qt.tail(4)[metric_col].median()
            historical = qt[metric_col].median()
            trend_direction = "worse" if recent > historical else "better"
        else:
            recent, historical, trend_direction = 0, 0, "unknown"
    else:
        recent, historical, trend_direction = 0, 0, "unknown"

    # Stage summary
    stage_summary = stage_duration_summary(df)

    # Policy impact
    impact = policy_impact_analysis(df)
    impact_df = pd.DataFrame(impact) if impact else pd.DataFrame()

    # Stuck permits for display
    stuck_display_cols = ["permit_number", "filed_date", "days_waiting", "supervisor_district",
                          "proposed_units", "description"]
    stuck_display = stuck[[c for c in stuck_display_cols if c in stuck.columns]].head(20)

    # Render
    html = REPORT_TEMPLATE.render(
        generated_date=datetime.now().strftime("%B %d, %Y"),
        total_permits=len(df),
        total_housing=len(housing),
        median_days_to_issue=median_days if pd.notna(median_days) else 0,
        stuck_count=len(stuck),
        stuck_units=stuck_units,
        worst_district=worst_district,
        worst_district_days=worst_district_days,
        trend_direction=trend_direction,
        recent_median=recent,
        historical_median=historical,
        district_table=_df_to_html(scorecard),
        stage_table=_df_to_html(stage_summary),
        policy_impact=not impact_df.empty,
        policy_table=_df_to_html(impact_df) if not impact_df.empty else "",
        stuck_table=_df_to_html(stuck_display),
    )

    output_path = REPORTS_DIR / "bottleneck_report.html"
    output_path.write_text(html)
    print(f"Report generated: {output_path}")
    return output_path


if __name__ == "__main__":
    df = pd.read_parquet(PROCESSED_DIR / "building_permits.parquet")
    for col in ["filed_date", "approved_date", "issued_date", "completed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    generate_report(df)
