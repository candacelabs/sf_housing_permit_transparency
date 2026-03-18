"""SF Permitting Bottleneck Analyzer — Interactive Dash Dashboard.

Designed for policymakers: clear, simple, impactful.
"""
import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, callback, Input, Output, State
import dash_bootstrap_components as dbc
from pathlib import Path

logger = logging.getLogger(__name__)

from src.config import (
    PROCESSED_DIR, DASH_HOST, DASH_PORT, DASH_DEBUG,
    POLICY_MILESTONES, STAGE_DATE_COLUMNS,
)
from src.analysis.bottlenecks import (
    stage_duration_summary, worst_bottlenecks, permit_status_breakdown,
    stuck_permits, volume_analysis, district_scorecard,
)
from src.analysis.trends import (
    quarterly_trends, annual_trends, policy_impact_analysis,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data() -> pd.DataFrame:
    """Load cleaned building permits from parquet cache."""
    path = PROCESSED_DIR / "building_permits.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"No processed data at {path}. Run the pipeline first:\n"
            "  uv run python -m src.ingestion.fetch\n"
            "  uv run python -m src.ingestion.clean"
        )
    logger.info("Loading processed data from %s", path)
    df = pd.read_parquet(path)
    # Ensure date columns are datetime
    for col in ["filed_date", "approved_date", "issued_date", "completed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="SF Permitting Bottleneck Analyzer",
    suppress_callback_exceptions=True,
)

# ---------------------------------------------------------------------------
# Reusable components
# ---------------------------------------------------------------------------

def kpi_card(title: str, value: str, subtitle: str = "", color: str = "primary"):
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, className="card-subtitle mb-1 text-muted",
                     style={"fontSize": "0.85rem"}),
            html.H3(value, className=f"card-title text-{color} mb-0",
                     style={"fontWeight": "700"}),
            html.Small(subtitle, className="text-muted") if subtitle else None,
        ]),
        className="shadow-sm h-100",
    )


def section_header(title: str, subtitle: str = ""):
    return html.Div([
        html.H4(title, className="mb-0", style={"fontWeight": "600"}),
        html.P(subtitle, className="text-muted mb-3") if subtitle else None,
    ], className="mt-4 mb-3")


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

def build_filters(df: pd.DataFrame):
    districts = sorted(df["supervisor_district"].dropna().unique().tolist())
    years = sorted(df["filed_year"].dropna().unique().tolist())
    min_year, max_year = int(years[0]) if years else 2000, int(years[-1]) if years else 2026

    return dbc.Card(dbc.CardBody([
        html.H5("Filters", className="mb-3"),
        dbc.Label("Supervisor District"),
        dcc.Dropdown(
            id="filter-district",
            options=[{"label": f"District {d}", "value": d} for d in districts],
            multi=True,
            placeholder="All districts",
        ),
        dbc.Label("Year Range", className="mt-3"),
        dcc.RangeSlider(
            id="filter-year-range",
            min=min_year,
            max=max_year,
            value=[max(min_year, 2015), max_year],
            marks={y: str(y) for y in range(min_year, max_year + 1, 5)},
            step=1,
            tooltip={"placement": "bottom", "always_visible": True},
        ),
        dbc.Label("Housing Permits Only", className="mt-3"),
        dbc.Switch(id="filter-housing-only", value=True, label="Yes"),
    ]), className="shadow-sm")


def build_layout(df: pd.DataFrame):
    housing = df[df["is_housing"] == True]
    stuck = stuck_permits(df)
    scorecard = district_scorecard(df)

    # KPIs
    total_permits = len(housing)
    median_days = housing["days_filed_to_issued"].median()
    total_stuck = len(stuck)
    stuck_units = stuck["proposed_units"].sum() if "proposed_units" in stuck.columns else 0

    return dbc.Container([
        # Header
        dbc.Row(dbc.Col(html.Div([
            html.H2("SF Permitting Bottleneck Analyzer",
                     className="mb-0", style={"fontWeight": "700"}),
            html.P(
                "Making San Francisco's housing permitting pipeline transparent. "
                "Data from SF Open Data (DBI Building Permits).",
                className="text-muted mb-0",
            ),
        ]), className="py-3 border-bottom mb-3")),

        # Filters + KPIs row
        dbc.Row([
            dbc.Col(build_filters(df), width=3),
            dbc.Col([
                dbc.Row([
                    dbc.Col(kpi_card(
                        "Housing Permits",
                        f"{total_permits:,}",
                        "Total in dataset",
                    ), md=3),
                    dbc.Col(kpi_card(
                        "Median Days to Issue",
                        f"{median_days:,.0f}" if pd.notna(median_days) else "N/A",
                        "Filed to permit issued",
                        color="warning" if pd.notna(median_days) and median_days > 180 else "success",
                    ), md=3),
                    dbc.Col(kpi_card(
                        "Stuck Permits",
                        f"{total_stuck:,}",
                        f"Filed >1yr, not issued",
                        color="danger",
                    ), md=3),
                    dbc.Col(kpi_card(
                        "Units Stuck",
                        f"{stuck_units:,.0f}",
                        "Housing units blocked",
                        color="danger",
                    ), md=3),
                ], className="mb-3"),

                # Tabs for main content
                dbc.Tabs([
                    dbc.Tab(label="Bottlenecks", tab_id="tab-bottlenecks"),
                    dbc.Tab(label="Trends", tab_id="tab-trends"),
                    dbc.Tab(label="District Scorecard", tab_id="tab-districts"),
                    dbc.Tab(label="Stuck Permits", tab_id="tab-stuck"),
                ], id="main-tabs", active_tab="tab-bottlenecks"),
                html.Div(id="tab-content", className="mt-3"),
            ], width=9),
        ]),
    ], fluid=True, className="px-4")


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def apply_filters(df, districts, year_range, housing_only):
    """Apply user-selected filters to the DataFrame."""
    filtered = df.copy()
    if districts:
        filtered = filtered[filtered["supervisor_district"].isin(districts)]
    if year_range:
        filtered = filtered[
            (filtered["filed_year"] >= year_range[0]) &
            (filtered["filed_year"] <= year_range[1])
        ]
    if housing_only:
        filtered = filtered[filtered["is_housing"] == True]
    return filtered


def register_callbacks(df: pd.DataFrame):

    @callback(
        Output("tab-content", "children"),
        Input("main-tabs", "active_tab"),
        Input("filter-district", "value"),
        Input("filter-year-range", "value"),
        Input("filter-housing-only", "value"),
    )
    def render_tab(tab, districts, year_range, housing_only):
        filtered = apply_filters(df, districts, year_range, housing_only)

        if len(filtered) == 0:
            return dbc.Alert("No permits match the current filters.", color="warning")

        if tab == "tab-bottlenecks":
            return render_bottlenecks(filtered)
        elif tab == "tab-trends":
            return render_trends(filtered)
        elif tab == "tab-districts":
            return render_districts(filtered)
        elif tab == "tab-stuck":
            return render_stuck(filtered)
        return html.Div()


def render_bottlenecks(df):
    """Bottleneck analysis tab."""
    # Stage duration box plots
    duration_cols = [
        ("days_filed_to_approved", "Filed to Approved"),
        ("days_approved_to_issued", "Approved to Issued"),
        ("days_filed_to_issued", "Filed to Issued"),
        ("days_issued_to_completed", "Issued to Completed"),
    ]
    box_data = []
    for col, label in duration_cols:
        if col in df.columns:
            vals = df[col].dropna()
            # Cap at 99th percentile for visualization
            cap = vals.quantile(0.99) if len(vals) > 0 else 1000
            vals = vals[vals <= cap]
            for v in vals:
                box_data.append({"Stage": label, "Days": v})

    box_df = pd.DataFrame(box_data)
    fig_box = px.box(
        box_df, x="Stage", y="Days",
        title="Permit Processing Time by Stage",
        color="Stage",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_box.update_layout(showlegend=False, height=400)

    # By-district bar chart — stage_duration_summary returns "stage", group_col, "median", etc.
    # Filter to the filed-to-issued stage for the district chart
    summary = stage_duration_summary(df, group_by="supervisor_district")
    district_summary = summary[summary["stage"] == "days_filed_to_issued"] if not summary.empty else pd.DataFrame()
    if not district_summary.empty and "median" in district_summary.columns:
        district_sorted = district_summary.sort_values("median", ascending=False)
        fig_district = px.bar(
            district_sorted,
            x="supervisor_district",
            y="median",
            title="Median Days: Filed to Issued (by Supervisor District)",
            labels={"supervisor_district": "District", "median": "Median Days"},
            color="median",
            color_continuous_scale="RdYlGn_r",
        )
        fig_district.update_layout(height=400)
    else:
        fig_district = go.Figure()
        fig_district.add_annotation(text="Insufficient data", showarrow=False)

    # By permit type
    summary_type = stage_duration_summary(df, group_by="permit_type_definition")
    type_summary = summary_type[summary_type["stage"] == "days_filed_to_issued"] if not summary_type.empty else pd.DataFrame()
    if not type_summary.empty and "median" in type_summary.columns:
        top_types = type_summary.nlargest(15, "median")
        fig_type = px.bar(
            top_types,
            y="permit_type_definition",
            x="median",
            orientation="h",
            title="Slowest Permit Types (Median Days to Issuance)",
            labels={"permit_type_definition": "Permit Type", "median": "Median Days"},
            color="median",
            color_continuous_scale="RdYlGn_r",
        )
        fig_type.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
    else:
        fig_type = go.Figure()

    return html.Div([
        section_header("Where Do Permits Get Stuck?",
                       "Distribution of processing times across pipeline stages"),
        dcc.Graph(figure=fig_box),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_district), md=6),
            dbc.Col(dcc.Graph(figure=fig_type), md=6),
        ]),
    ])


def render_trends(df):
    """Trend analysis tab."""
    # Quarterly trend line
    qt = quarterly_trends(df)
    if not qt.empty:
        fig_quarterly = go.Figure()
        metric_col = [c for c in qt.columns if c.startswith("median_")][0] if any(c.startswith("median_") for c in qt.columns) else None
        rolling_col = [c for c in qt.columns if "rolling" in c.lower()][0] if any("rolling" in c.lower() for c in qt.columns) else None

        if metric_col:
            fig_quarterly.add_trace(go.Scatter(
                x=qt["period"], y=qt[metric_col],
                mode="lines+markers", name="Quarterly Median",
                line=dict(color="#3498db", width=1),
                marker=dict(size=4),
                opacity=0.6,
            ))
        if rolling_col:
            fig_quarterly.add_trace(go.Scatter(
                x=qt["period"], y=qt[rolling_col],
                mode="lines", name="4-Quarter Rolling Avg",
                line=dict(color="#e74c3c", width=3),
            ))

        # Add policy milestone annotations
        for date_str, event in POLICY_MILESTONES.items():
            date = pd.Timestamp(date_str)
            # Find closest period
            qt_dates = pd.to_datetime(qt["period"].str.replace(r"(\d{4})-Q(\d)",
                lambda m: f"{m.group(1)}-{int(m.group(2))*3:02d}-01", regex=True), errors="coerce")
            if len(qt_dates.dropna()) > 0:
                fig_quarterly.add_vline(
                    x=date_str[:4] + "-Q" + str((int(date_str[5:7]) - 1) // 3 + 1),
                    line_dash="dash", line_color="gray", opacity=0.5,
                    annotation_text=event[:30],
                    annotation_position="top",
                    annotation_font_size=9,
                )

        fig_quarterly.update_layout(
            title="Median Days: Filed to Issued (Quarterly Trend)",
            xaxis_title="Quarter", yaxis_title="Median Days",
            height=450, hovermode="x unified",
        )
    else:
        fig_quarterly = go.Figure()
        fig_quarterly.add_annotation(text="Insufficient data", showarrow=False)

    # Annual volume chart
    vol = volume_analysis(df, group_by="filed_year")
    if not vol.empty:
        fig_volume = go.Figure()
        if "permits_filed" in vol.columns:
            fig_volume.add_trace(go.Bar(x=vol["filed_year"], y=vol["permits_filed"], name="Filed"))
        if "permits_issued" in vol.columns:
            fig_volume.add_trace(go.Bar(x=vol["filed_year"], y=vol["permits_issued"], name="Issued"))
        if "permits_completed" in vol.columns:
            fig_volume.add_trace(go.Bar(x=vol["filed_year"], y=vol["permits_completed"], name="Completed"))
        fig_volume.update_layout(
            title="Housing Permit Volume by Year",
            barmode="group", height=400,
            xaxis_title="Year", yaxis_title="Number of Permits",
        )
    else:
        fig_volume = go.Figure()

    # Policy impact table
    impact = policy_impact_analysis(df)
    if impact:
        impact_df = pd.DataFrame(impact)
        impact_table = dbc.Table.from_dataframe(
            impact_df.round(1),
            striped=True, bordered=True, hover=True, size="sm",
        )
    else:
        impact_table = dbc.Alert("Not enough data for policy impact analysis.", color="info")

    return html.Div([
        section_header("How Have Processing Times Changed?",
                       "Quarterly and annual trends with key policy milestones"),
        dcc.Graph(figure=fig_quarterly),
        dcc.Graph(figure=fig_volume),
        section_header("Policy Impact Analysis",
                       "Did key policy changes actually speed things up?"),
        impact_table,
    ])


def render_districts(df):
    """District scorecard tab."""
    scorecard = district_scorecard(df)
    if scorecard.empty:
        return dbc.Alert("No district data available.", color="warning")

    # Scorecard table
    display_cols = [c for c in scorecard.columns if c != "bottleneck_score"]
    table = dbc.Table.from_dataframe(
        scorecard[display_cols].round(1),
        striped=True, bordered=True, hover=True, size="sm",
        className="mt-2",
    )

    # Choropleth-style bar chart
    if "median_days_to_issuance" in scorecard.columns:
        fig = px.bar(
            scorecard.sort_values("median_days_to_issuance", ascending=True),
            y="supervisor_district",
            x="median_days_to_issuance",
            orientation="h",
            title="District Rankings: Median Days to Permit Issuance",
            color="median_days_to_issuance",
            color_continuous_scale="RdYlGn_r",
            labels={"supervisor_district": "District", "median_days_to_issuance": "Median Days"},
        )
        fig.update_layout(height=450)
    else:
        fig = go.Figure()

    # Units stuck per district
    stuck = stuck_permits(df)
    if not stuck.empty and "supervisor_district" in stuck.columns:
        stuck_by_dist = stuck.groupby("supervisor_district").agg(
            permits_stuck=("permit_number", "count"),
            units_stuck=("proposed_units", "sum"),
        ).reset_index().sort_values("units_stuck", ascending=False)

        fig_stuck = px.bar(
            stuck_by_dist,
            x="supervisor_district",
            y="units_stuck",
            title="Housing Units Stuck in Pipeline (by District)",
            labels={"supervisor_district": "District", "units_stuck": "Units Stuck"},
            color="units_stuck",
            color_continuous_scale="Reds",
            text="permits_stuck",
        )
        fig_stuck.update_traces(texttemplate="%{text} permits", textposition="outside")
        fig_stuck.update_layout(height=400)
    else:
        fig_stuck = go.Figure()

    return html.Div([
        section_header("District Scorecard",
                       "How does each Supervisor District compare?"),
        dcc.Graph(figure=fig),
        dcc.Graph(figure=fig_stuck),
        section_header("Detailed Scorecard"),
        table,
    ])


def render_stuck(df):
    """Stuck permits tab — the money shot for policymakers."""
    stuck = stuck_permits(df)
    if stuck.empty:
        return dbc.Alert("No stuck permits found with current filters.", color="success")

    total_stuck = len(stuck)
    total_units = stuck["proposed_units"].sum() if "proposed_units" in stuck.columns else 0
    avg_wait = stuck["days_waiting"].mean() if "days_waiting" in stuck.columns else 0

    # Summary cards
    cards = dbc.Row([
        dbc.Col(kpi_card("Stuck Permits", f"{total_stuck:,}", ">1 year without issuance", "danger"), md=4),
        dbc.Col(kpi_card("Units Blocked", f"{total_units:,.0f}", "Housing units that can't break ground", "danger"), md=4),
        dbc.Col(kpi_card("Avg Wait", f"{avg_wait:,.0f} days", f"({avg_wait/365:.1f} years)", "warning"), md=4),
    ], className="mb-3")

    # Top 50 worst stuck permits table
    display_cols = ["permit_number", "filed_date", "status", "days_waiting",
                    "supervisor_district", "proposed_units", "description"]
    available_cols = [c for c in display_cols if c in stuck.columns]
    top_stuck = stuck.head(50)[available_cols].copy()
    if "filed_date" in top_stuck.columns:
        top_stuck["filed_date"] = pd.to_datetime(top_stuck["filed_date"]).dt.strftime("%Y-%m-%d")
    if "days_waiting" in top_stuck.columns:
        top_stuck["years_waiting"] = (top_stuck["days_waiting"] / 365).round(1)
    if "description" in top_stuck.columns:
        top_stuck["description"] = top_stuck["description"].str[:100]

    table = dbc.Table.from_dataframe(
        top_stuck, striped=True, bordered=True, hover=True, size="sm",
    )

    # Histogram of wait times
    fig_hist = px.histogram(
        stuck, x="days_waiting", nbins=50,
        title="Distribution of Wait Times for Stuck Permits",
        labels={"days_waiting": "Days Waiting"},
        color_discrete_sequence=["#e74c3c"],
    )
    fig_hist.update_layout(height=350)

    return html.Div([
        section_header(
            f"{total_stuck:,} Permits Stuck in the Pipeline",
            f"These permits were filed over a year ago and still haven't been issued. "
            f"They represent {total_units:,.0f} potential housing units.",
        ),
        cards,
        dcc.Graph(figure=fig_hist),
        section_header("Worst Offenders (Top 50 by Wait Time)"),
        html.Div(table, style={"maxHeight": "500px", "overflowY": "auto"}),
    ])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def create_app():
    """Create and configure the Dash app."""
    df = load_data()
    app.layout = build_layout(df)
    register_callbacks(df)
    logger.info("Dashboard app created and ready to serve")
    return app


if __name__ == "__main__":
    application = create_app()
    application.run(host=DASH_HOST, port=DASH_PORT, debug=DASH_DEBUG)
