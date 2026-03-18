"""SF Permitting Bottleneck Analyzer — FastAPI + Plotly.js dashboard.

All data queries go through DuckDB (20-120ms per query on 1.3M rows).
The server starts instantly; computation happens per-request.
"""
import json
import logging
from pathlib import Path

import plotly
import plotly.express as px
import plotly.graph_objects as go
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from src.config import POLICY_MILESTONES
from src.analysis import queries

logger = logging.getLogger(__name__)

app = FastAPI(title="SF Permitting Bottleneck Analyzer")

_jinja_env = Environment()
_jinja_env.filters["comma"] = lambda v: f"{int(v):,}" if v is not None and v == v else str(v)

TEMPLATE = _jinja_env.from_string("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SF Permitting Bottleneck Analyzer</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f6fa; color: #2c3e50; }
  .header { background: #1a5276; color: white; padding: 20px 32px; }
  .header h1 { font-size: 1.5rem; font-weight: 700; }
  .header p { opacity: 0.8; font-size: 0.9rem; margin-top: 4px; }
  .container { max-width: 1400px; margin: 0 auto; padding: 20px 32px; }
  .filters { background: white; border-radius: 8px; padding: 16px 20px; margin-bottom: 20px; display: flex; gap: 20px; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.08); flex-wrap: wrap; }
  .filters label { font-weight: 600; font-size: 0.85rem; color: #7f8c8d; }
  .filters select, .filters input { padding: 6px 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 0.9rem; }
  .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
  .kpi { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-left: 4px solid #3498db; }
  .kpi.warn { border-left-color: #f39c12; }
  .kpi.danger { border-left-color: #e74c3c; }
  .kpi-value { font-size: 2rem; font-weight: 700; }
  .kpi-label { font-size: 0.85rem; color: #7f8c8d; margin-top: 4px; }
  .tabs { display: flex; gap: 0; margin-bottom: 0; }
  .tab { padding: 10px 24px; cursor: pointer; font-weight: 600; color: #7f8c8d; border-bottom: 3px solid transparent; transition: all 0.15s; }
  .tab:hover { color: #2c3e50; }
  .tab.active { color: #1a5276; border-bottom-color: #1a5276; }
  .tab-content { background: white; border-radius: 0 8px 8px 8px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); min-height: 400px; }
  .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 16px; }
  table { border-collapse: collapse; width: 100%; margin-top: 16px; font-size: 0.85rem; }
  th { background: #2c3e50; color: white; padding: 8px 12px; text-align: left; }
  td { padding: 8px 12px; border-bottom: 1px solid #eee; }
  tr:hover { background: #f8f9fa; }
  .section-title { font-size: 1.1rem; font-weight: 600; margin: 20px 0 8px; }
  .section-subtitle { color: #7f8c8d; font-size: 0.85rem; margin-bottom: 12px; }
  .loading { text-align: center; padding: 60px; color: #95a5a6; }
  @media (max-width: 768px) { .kpi-grid { grid-template-columns: 1fr 1fr; } .chart-row { grid-template-columns: 1fr; } }
</style>
</head>
<body>

<div class="header">
  <h1>SF Permitting Bottleneck Analyzer</h1>
  <p>Making San Francisco's housing permitting pipeline transparent. Data: DBI Building Permits ({{ kpis.total_permits | comma }} permits).</p>
</div>

<div class="container">

  <div class="filters">
    <div>
      <label>District</label><br>
      <select id="f-district" onchange="refresh()">
        <option value="">All Districts</option>
        {% for d in filters.districts %}<option value="{{ d }}">District {{ d }}</option>{% endfor %}
      </select>
    </div>
    <div>
      <label>Year From</label><br>
      <input type="number" id="f-year-min" value="{{ filters.min_year | int }}" min="{{ filters.min_year | int }}" max="{{ filters.max_year | int }}" onchange="refresh()" style="width:80px">
    </div>
    <div>
      <label>Year To</label><br>
      <input type="number" id="f-year-max" value="{{ filters.max_year | int }}" min="{{ filters.min_year | int }}" max="{{ filters.max_year | int }}" onchange="refresh()" style="width:80px">
    </div>
    <div>
      <label>Housing Only</label><br>
      <select id="f-housing" onchange="refresh()">
        <option value="1" selected>Yes</option>
        <option value="0">No</option>
      </select>
    </div>
  </div>

  <div class="kpi-grid" id="kpi-grid">
    <div class="kpi"><div class="kpi-value" id="kpi-permits">{{ kpis.total_permits | comma }}</div><div class="kpi-label">Housing Permits</div></div>
    <div class="kpi warn"><div class="kpi-value" id="kpi-days">{{ kpis.median_days_to_issue }}</div><div class="kpi-label">Median Days to Issue</div></div>
    <div class="kpi danger"><div class="kpi-value" id="kpi-stuck">{{ kpis.stuck_count | comma }}</div><div class="kpi-label">Stuck Permits (&gt;1yr)</div></div>
    <div class="kpi danger"><div class="kpi-value" id="kpi-units">{{ kpis.stuck_units | comma }}</div><div class="kpi-label">Housing Units Blocked</div></div>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('bottlenecks')">Bottlenecks</div>
    <div class="tab" onclick="switchTab('trends')">Trends</div>
    <div class="tab" onclick="switchTab('districts')">District Scorecard</div>
    <div class="tab" onclick="switchTab('stuck')">Stuck Permits</div>
  </div>

  <div class="tab-content" id="tab-content">
    <div class="loading">Loading...</div>
  </div>

</div>

<script>
let currentTab = 'bottlenecks';
const comma = n => n == null ? 'N/A' : Number(n).toLocaleString();

function getFilters() {
  const d = document.getElementById('f-district').value;
  const p = new URLSearchParams();
  if (d) p.set('district', d);
  p.set('year_min', document.getElementById('f-year-min').value);
  p.set('year_max', document.getElementById('f-year-max').value);
  p.set('housing_only', document.getElementById('f-housing').value);
  return p.toString();
}

async function refresh() {
  const q = getFilters();
  // Update KPIs
  const kpis = await (await fetch('/api/kpis?' + q)).json();
  document.getElementById('kpi-permits').textContent = comma(kpis.total_permits);
  document.getElementById('kpi-days').textContent = kpis.median_days_to_issue ?? 'N/A';
  document.getElementById('kpi-stuck').textContent = comma(kpis.stuck_count);
  document.getElementById('kpi-units').textContent = comma(kpis.stuck_units);
  // Update current tab
  loadTab(currentTab);
}

function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  event.target.classList.add('active');
  loadTab(tab);
}

async function loadTab(tab) {
  const q = getFilters();
  const el = document.getElementById('tab-content');
  el.innerHTML = '<div class="loading">Loading...</div>';

  if (tab === 'bottlenecks') {
    const [stages, districts, types] = await Promise.all([
      fetch('/api/stages?' + q).then(r => r.json()),
      fetch('/api/by_district?' + q).then(r => r.json()),
      fetch('/api/by_permit_type?' + q).then(r => r.json()),
    ]);
    el.innerHTML = '<div id="chart-stages"></div><div class="chart-row"><div id="chart-districts"></div><div id="chart-types"></div></div>';
    Plotly.newPlot('chart-stages', stages.data, stages.layout, {responsive: true});
    Plotly.newPlot('chart-districts', districts.data, districts.layout, {responsive: true});
    Plotly.newPlot('chart-types', types.data, types.layout, {responsive: true});
  }
  else if (tab === 'trends') {
    const [quarterly, volume, impact] = await Promise.all([
      fetch('/api/quarterly?' + q).then(r => r.json()),
      fetch('/api/annual_volume?' + q).then(r => r.json()),
      fetch('/api/policy_impact').then(r => r.json()),
    ]);
    el.innerHTML = '<div id="chart-quarterly"></div><div id="chart-volume"></div><h3 class="section-title" style="margin-top:24px">Policy Impact Analysis</h3><p class="section-subtitle">Did key policy changes actually speed things up?</p><div id="policy-table"></div>';
    Plotly.newPlot('chart-quarterly', quarterly.data, quarterly.layout, {responsive: true});
    Plotly.newPlot('chart-volume', volume.data, volume.layout, {responsive: true});
    // Render policy impact table
    let html = '<table><tr><th>Date</th><th>Event</th><th>Median Before</th><th>Median After</th><th>Change</th></tr>';
    for (const p of impact) {
      const color = p.pct_change != null ? (p.pct_change < 0 ? 'color:#27ae60' : 'color:#e74c3c') : '';
      html += '<tr><td>' + p.date + '</td><td>' + p.event + '</td><td>' + (p.median_before ?? '-') + '</td><td>' + (p.median_after ?? '-') + '</td><td style="' + color + '">' + (p.pct_change != null ? p.pct_change + '%' : '-') + '</td></tr>';
    }
    html += '</table>';
    document.getElementById('policy-table').innerHTML = html;
  }
  else if (tab === 'districts') {
    const [scorecard, stuckDist] = await Promise.all([
      fetch('/api/by_district?' + q).then(r => r.json()),
      fetch('/api/stuck_by_district?' + q).then(r => r.json()),
    ]);
    el.innerHTML = '<div id="chart-scorecard"></div><div id="chart-stuck-dist"></div><h3 class="section-title">Detailed Scorecard</h3><div id="scorecard-table"></div>';
    Plotly.newPlot('chart-scorecard', scorecard.data, scorecard.layout, {responsive: true});
    Plotly.newPlot('chart-stuck-dist', stuckDist.data, stuckDist.layout, {responsive: true});
    // Table
    const data = await fetch('/api/district_table?' + q).then(r => r.json());
    let html = '<table><tr><th>District</th><th>Median Days</th><th>Permits</th><th>Units Proposed</th></tr>';
    for (const r of data) html += '<tr><td>' + r.district + '</td><td>' + (r.median_days?.toFixed(1) ?? '-') + '</td><td>' + comma(r.permits) + '</td><td>' + comma(r.units_proposed) + '</td></tr>';
    html += '</table>';
    document.getElementById('scorecard-table').innerHTML = html;
  }
  else if (tab === 'stuck') {
    const [stuck, kpis] = await Promise.all([
      fetch('/api/stuck_list?' + q).then(r => r.json()),
      fetch('/api/kpis?' + q).then(r => r.json()),
    ]);
    el.innerHTML = '<h3 class="section-title">' + comma(kpis.stuck_count) + ' Permits Stuck in the Pipeline</h3><p class="section-subtitle">Filed over a year ago, still not issued. Representing ' + comma(kpis.stuck_units) + ' potential housing units.</p><div id="stuck-table"></div>';
    let html = '<table><tr><th>Permit</th><th>Filed</th><th>Status</th><th>Days Waiting</th><th>District</th><th>Neighborhood</th><th>Units</th><th>Description</th></tr>';
    for (const r of stuck) {
      const filed = r.filed_date ? new Date(r.filed_date).toISOString().slice(0,10) : '-';
      html += '<tr><td>' + (r.permit_number ?? '') + '</td><td>' + filed + '</td><td>' + (r.status ?? '') + '</td><td>' + comma(r.days_waiting) + '</td><td>' + (r.district ?? '') + '</td><td>' + (r.neighborhood ?? '') + '</td><td>' + (r.units ?? '') + '</td><td>' + (r.description ?? '') + '</td></tr>';
    }
    html += '</table>';
    document.getElementById('stuck-table').innerHTML = html;
  }
}

// Initial load
loadTab('bottlenecks');
</script>
</body>
</html>""")


# ---------------------------------------------------------------------------
# Template filters
# ---------------------------------------------------------------------------

def _comma(v):
    try:
        return f"{int(v):,}"
    except (ValueError, TypeError):
        return str(v)


def _to_json(fig):
    """Convert a Plotly figure to JSON dict with data + layout."""
    return json.loads(plotly.io.to_json(fig))


# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    k = queries.kpis()
    f = queries.filter_options()
    return TEMPLATE.render(kpis=k, filters=f)


# ---------------------------------------------------------------------------
# API endpoints — each returns Plotly JSON or raw data
# ---------------------------------------------------------------------------

def _parse_filters(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
) -> dict:
    districts = [district] if district else None
    return dict(districts=districts, year_min=year_min, year_max=year_max, housing_only=bool(housing_only))


@app.get("/api/kpis")
async def api_kpis(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    return queries.kpis(**f)


@app.get("/api/stages")
async def api_stages(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    df = queries.stage_durations(**f)
    fig = px.bar(df, x="stage", y="median_days", color="stage",
                 title="Median Processing Time by Stage (days)",
                 text="median_days",
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig.update_layout(showlegend=False, height=350)
    return _to_json(fig)


@app.get("/api/by_district")
async def api_by_district(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    df = queries.by_district(**f)
    fig = px.bar(df, x="district", y="median_days",
                 title="Median Days: Filed to Issued (by District)",
                 color="median_days", color_continuous_scale="RdYlGn_r")
    fig.update_layout(height=400)
    return _to_json(fig)


@app.get("/api/by_permit_type")
async def api_by_permit_type(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    df = queries.by_permit_type(**f)
    fig = px.bar(df, y="permit_type", x="median_days", orientation="h",
                 title="Slowest Permit Types (Median Days)",
                 color="median_days", color_continuous_scale="RdYlGn_r")
    fig.update_layout(height=400, yaxis={"categoryorder": "total ascending"})
    return _to_json(fig)


@app.get("/api/quarterly")
async def api_quarterly(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    df = queries.quarterly_trends(**f)
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(go.Scatter(x=df["period"], y=df["median_days"],
                                  mode="lines+markers", name="Quarterly Median",
                                  line=dict(color="#3498db", width=1), marker=dict(size=3), opacity=0.6))
        fig.add_trace(go.Scatter(x=df["period"], y=df["rolling_avg"],
                                  mode="lines", name="4-Qtr Rolling Avg",
                                  line=dict(color="#e74c3c", width=3)))
        for date_str, event in POLICY_MILESTONES.items():
            q = str((int(date_str[5:7]) - 1) // 3 + 1)
            period = date_str[:4] + "-Q" + q
            if period in df["period"].values:
                fig.add_annotation(x=period, y=1, yref="paper",
                                    text=event[:25], showarrow=True, arrowhead=2,
                                    font=dict(size=8, color="gray"), arrowcolor="gray")
    fig.update_layout(title="Median Days: Filed to Issued (Quarterly)", height=450, hovermode="x unified")
    return _to_json(fig)


@app.get("/api/annual_volume")
async def api_annual_volume(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    df = queries.annual_volume(**f)
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(go.Bar(x=df["year"], y=df["filed"], name="Filed"))
        fig.add_trace(go.Bar(x=df["year"], y=df["issued"], name="Issued"))
        fig.add_trace(go.Bar(x=df["year"], y=df["completed"], name="Completed"))
    fig.update_layout(title="Permit Volume by Year", barmode="group", height=400)
    return _to_json(fig)


@app.get("/api/policy_impact")
async def api_policy_impact():
    return queries.policy_impact()


@app.get("/api/stuck_by_district")
async def api_stuck_by_district(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
):
    df = queries.stuck_by_district(
        districts=[district] if district else None,
        year_min=year_min, year_max=year_max,
    )
    fig = px.bar(df, x="district", y="stuck_units",
                 title="Housing Units Stuck in Pipeline (by District)",
                 color="stuck_units", color_continuous_scale="Reds",
                 text="stuck_permits")
    fig.update_traces(texttemplate="%{text} permits", textposition="outside")
    fig.update_layout(height=400)
    return _to_json(fig)


@app.get("/api/district_table")
async def api_district_table(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
    housing_only: int = Query(1),
):
    f = _parse_filters(district, year_min, year_max, housing_only)
    df = queries.by_district(**f)
    return df.to_dict(orient="records")


@app.get("/api/stuck_list")
async def api_stuck_list(
    district: str | None = Query(None),
    year_min: int | None = Query(None),
    year_max: int | None = Query(None),
):
    df = queries.stuck_permits_list(
        districts=[district] if district else None,
        year_min=year_min, year_max=year_max,
    )
    # Replace NaN/NaT with None for JSON serialization
    return json.loads(df.to_json(orient="records", date_format="iso"))
