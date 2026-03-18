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
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<script>
tailwind.config = {
  theme: {
    extend: {
      colors: {
        navy: { 50: '#f0f5fa', 100: '#d9e6f2', 500: '#1a5276', 600: '#154360', 700: '#0e2f46', 800: '#0a1f2e' },
      }
    }
  }
}
</script>
<style type="text/tailwindcss">
  @layer components {
    .kpi-card { @apply bg-white rounded-xl p-5 shadow-sm border-l-4 transition-all duration-200 hover:shadow-md; }
    .filter-input { @apply block w-full rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm focus:border-navy-500 focus:ring-2 focus:ring-navy-500/20 focus:outline-none transition-colors; }
    .data-table { @apply w-full text-sm text-left; }
    .data-table th { @apply bg-gray-800 text-white px-4 py-3 font-medium first:rounded-tl-lg last:rounded-tr-lg; }
    .data-table td { @apply px-4 py-3 border-b border-gray-100; }
    .data-table tr:hover td { @apply bg-gray-50; }
    .tab-btn { @apply px-5 py-2.5 text-sm font-semibold text-gray-400 border-b-2 border-transparent cursor-pointer transition-all duration-150 hover:text-gray-600; }
    .tab-btn.active { @apply text-navy-500 border-navy-500; }
    .badge { @apply inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium; }
    .badge-red { @apply bg-red-50 text-red-700 ring-1 ring-red-600/20; }
    .badge-green { @apply bg-emerald-50 text-emerald-700 ring-1 ring-emerald-600/20; }
  }
</style>
</head>
<body class="bg-gray-50 text-gray-900 antialiased">

<!-- Header -->
<header class="bg-navy-500 text-white">
  <div class="max-w-7xl mx-auto px-6 py-5 flex items-center justify-between">
    <div>
      <h1 class="text-xl font-bold tracking-tight">SF Permitting Bottleneck Analyzer</h1>
      <p class="text-navy-50/80 text-sm mt-0.5">Making San Francisco's housing permitting pipeline transparent</p>
    </div>
    <div class="text-right text-sm text-navy-50/60">
      <div>{{ kpis.total_permits | comma }} permits analyzed</div>
      <div>DBI Building Permits &middot; SF Open Data</div>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-6 py-6">

  <!-- Filters -->
  <div class="bg-white rounded-xl shadow-sm p-4 mb-6 flex flex-wrap items-end gap-4">
    <div class="flex items-center gap-1.5 text-sm font-semibold text-gray-500 uppercase tracking-wider">
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"/></svg>
      Filters
    </div>
    <div class="flex-1 min-w-[140px]">
      <label class="block text-xs font-medium text-gray-500 mb-1">Supervisor District</label>
      <select id="f-district" onchange="refresh()" class="filter-input">
        <option value="">All Districts</option>
        {% for d in filters.districts %}<option value="{{ d }}">District {{ d }}</option>{% endfor %}
      </select>
    </div>
    <div class="w-24">
      <label class="block text-xs font-medium text-gray-500 mb-1">Year From</label>
      <input type="number" id="f-year-min" value="{{ filters.min_year | int }}" min="{{ filters.min_year | int }}" max="{{ filters.max_year | int }}" onchange="refresh()" class="filter-input">
    </div>
    <div class="w-24">
      <label class="block text-xs font-medium text-gray-500 mb-1">Year To</label>
      <input type="number" id="f-year-max" value="{{ filters.max_year | int }}" min="{{ filters.min_year | int }}" max="{{ filters.max_year | int }}" onchange="refresh()" class="filter-input">
    </div>
    <div class="w-32">
      <label class="block text-xs font-medium text-gray-500 mb-1">Housing Only</label>
      <select id="f-housing" onchange="refresh()" class="filter-input">
        <option value="1" selected>Yes</option>
        <option value="0">No</option>
      </select>
    </div>
  </div>

  <!-- KPIs -->
  <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
    <div class="kpi-card border-blue-500">
      <p class="text-xs font-medium text-gray-500 uppercase tracking-wider">Housing Permits</p>
      <p class="text-3xl font-bold text-gray-900 mt-1" id="kpi-permits">{{ kpis.total_permits | comma }}</p>
      <p class="text-xs text-gray-400 mt-1">Total in filtered dataset</p>
    </div>
    <div class="kpi-card border-amber-500">
      <p class="text-xs font-medium text-gray-500 uppercase tracking-wider">Median Days to Issue</p>
      <p class="text-3xl font-bold text-amber-600 mt-1" id="kpi-days">{{ kpis.median_days_to_issue }}</p>
      <p class="text-xs text-gray-400 mt-1">Filed &rarr; permit issued</p>
    </div>
    <div class="kpi-card border-red-500">
      <p class="text-xs font-medium text-gray-500 uppercase tracking-wider">Stuck Permits</p>
      <p class="text-3xl font-bold text-red-600 mt-1" id="kpi-stuck">{{ kpis.stuck_count | comma }}</p>
      <p class="text-xs text-gray-400 mt-1">Filed &gt;1 year, not issued</p>
    </div>
    <div class="kpi-card border-red-500">
      <p class="text-xs font-medium text-gray-500 uppercase tracking-wider">Units Blocked</p>
      <p class="text-3xl font-bold text-red-600 mt-1" id="kpi-units">{{ kpis.stuck_units | comma }}</p>
      <p class="text-xs text-gray-400 mt-1">Housing units that can't break ground</p>
    </div>
  </div>

  <!-- Tabs -->
  <div class="flex border-b border-gray-200 mb-0">
    <button class="tab-btn active" onclick="switchTab('bottlenecks', this)">Bottlenecks</button>
    <button class="tab-btn" onclick="switchTab('trends', this)">Trends</button>
    <button class="tab-btn" onclick="switchTab('districts', this)">District Scorecard</button>
    <button class="tab-btn" onclick="switchTab('stuck', this)">Stuck Permits</button>
  </div>

  <!-- Tab content -->
  <div id="tab-content" class="bg-white rounded-b-xl shadow-sm p-6 min-h-[420px]">
    <div class="flex items-center justify-center h-64 text-gray-400">
      <svg class="animate-spin h-6 w-6 mr-2" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg>
      Loading...
    </div>
  </div>

</main>

<footer class="max-w-7xl mx-auto px-6 py-6 text-center text-xs text-gray-400">
  Data: <a href="https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9" class="underline hover:text-gray-600">SF Open Data &mdash; DBI Building Permits</a>
  &middot; Built for policymakers by <a href="https://github.com/candacelabs/sf_housing_permit_transparency" class="underline hover:text-gray-600">candacelabs</a>
</footer>

<script>
let currentTab = 'bottlenecks';
const comma = n => n == null ? 'N/A' : Number(n).toLocaleString();
const spinner = `<div class="flex items-center justify-center h-64 text-gray-400"><svg class="animate-spin h-6 w-6 mr-2" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg>Loading...</div>`;

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
  const kpis = await (await fetch('/api/kpis?' + q)).json();
  document.getElementById('kpi-permits').textContent = comma(kpis.total_permits);
  document.getElementById('kpi-days').textContent = kpis.median_days_to_issue ?? 'N/A';
  document.getElementById('kpi-stuck').textContent = comma(kpis.stuck_count);
  document.getElementById('kpi-units').textContent = comma(kpis.stuck_units);
  loadTab(currentTab);
}

function switchTab(tab, el) {
  currentTab = tab;
  document.querySelectorAll('.tab-btn').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  loadTab(tab);
}

function makeTable(headers, rows) {
  let h = '<div class="overflow-x-auto mt-4"><table class="data-table"><thead><tr>';
  for (const th of headers) h += `<th>${th}</th>`;
  h += '</tr></thead><tbody>';
  for (const row of rows) {
    h += '<tr>';
    for (const td of row) h += `<td>${td}</td>`;
    h += '</tr>';
  }
  h += '</tbody></table></div>';
  return h;
}

async function loadTab(tab) {
  const q = getFilters();
  const el = document.getElementById('tab-content');
  el.innerHTML = spinner;

  if (tab === 'bottlenecks') {
    const [stages, districts, types] = await Promise.all([
      fetch('/api/stages?' + q).then(r => r.json()),
      fetch('/api/by_district?' + q).then(r => r.json()),
      fetch('/api/by_permit_type?' + q).then(r => r.json()),
    ]);
    el.innerHTML = `
      <h3 class="text-lg font-semibold text-gray-800">Where Do Permits Get Stuck?</h3>
      <p class="text-sm text-gray-500 mb-4">Processing time distribution across pipeline stages</p>
      <div id="chart-stages"></div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
        <div id="chart-districts"></div>
        <div id="chart-types"></div>
      </div>`;
    Plotly.newPlot('chart-stages', stages.data, {...stages.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
    Plotly.newPlot('chart-districts', districts.data, {...districts.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
    Plotly.newPlot('chart-types', types.data, {...types.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
  }
  else if (tab === 'trends') {
    const [quarterly, volume, impact] = await Promise.all([
      fetch('/api/quarterly?' + q).then(r => r.json()),
      fetch('/api/annual_volume?' + q).then(r => r.json()),
      fetch('/api/policy_impact').then(r => r.json()),
    ]);
    el.innerHTML = `
      <h3 class="text-lg font-semibold text-gray-800">How Have Processing Times Changed?</h3>
      <p class="text-sm text-gray-500 mb-4">Quarterly and annual trends with key policy milestones</p>
      <div id="chart-quarterly"></div>
      <div id="chart-volume" class="mt-6"></div>
      <h3 class="text-lg font-semibold text-gray-800 mt-8">Policy Impact Analysis</h3>
      <p class="text-sm text-gray-500 mb-2">Did key policy changes actually speed things up?</p>
      <div id="policy-table"></div>`;
    Plotly.newPlot('chart-quarterly', quarterly.data, {...quarterly.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
    Plotly.newPlot('chart-volume', volume.data, {...volume.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
    const rows = impact.map(p => {
      const badge = p.pct_change != null
        ? (p.pct_change < 0 ? `<span class="badge badge-green">${p.pct_change}%</span>` : `<span class="badge badge-red">+${p.pct_change}%</span>`)
        : '-';
      return [p.date, p.event, p.median_before ?? '-', p.median_after ?? '-', badge];
    });
    document.getElementById('policy-table').innerHTML = makeTable(['Date','Event','Median Before (days)','Median After (days)','Change'], rows);
  }
  else if (tab === 'districts') {
    const [scorecard, stuckDist] = await Promise.all([
      fetch('/api/by_district?' + q).then(r => r.json()),
      fetch('/api/stuck_by_district?' + q).then(r => r.json()),
    ]);
    el.innerHTML = `
      <h3 class="text-lg font-semibold text-gray-800">District Scorecard</h3>
      <p class="text-sm text-gray-500 mb-4">How does each Supervisor District compare?</p>
      <div id="chart-scorecard"></div>
      <div id="chart-stuck-dist" class="mt-6"></div>
      <h3 class="text-lg font-semibold text-gray-800 mt-8">Detailed Scorecard</h3>
      <div id="scorecard-table"></div>`;
    Plotly.newPlot('chart-scorecard', scorecard.data, {...scorecard.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
    Plotly.newPlot('chart-stuck-dist', stuckDist.data, {...stuckDist.layout, paper_bgcolor:'transparent', plot_bgcolor:'transparent'}, {responsive:true});
    const data = await fetch('/api/district_table?' + q).then(r => r.json());
    const rows = data.map(r => [r.district, r.median_days?.toFixed(1) ?? '-', comma(r.permits), comma(r.units_proposed)]);
    document.getElementById('scorecard-table').innerHTML = makeTable(['District','Median Days','Permits','Units Proposed'], rows);
  }
  else if (tab === 'stuck') {
    const [stuck, kpis] = await Promise.all([
      fetch('/api/stuck_list?' + q).then(r => r.json()),
      fetch('/api/kpis?' + q).then(r => r.json()),
    ]);
    el.innerHTML = `
      <div class="flex items-start gap-4 mb-6">
        <div class="flex-shrink-0 w-12 h-12 bg-red-100 rounded-xl flex items-center justify-center">
          <svg class="w-6 h-6 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"/></svg>
        </div>
        <div>
          <h3 class="text-lg font-semibold text-gray-800">${comma(kpis.stuck_count)} Permits Stuck in the Pipeline</h3>
          <p class="text-sm text-gray-500">Filed over a year ago and still not issued, representing <span class="font-semibold text-red-600">${comma(kpis.stuck_units)} potential housing units</span>.</p>
        </div>
      </div>
      <div id="stuck-table"></div>`;
    const rows = stuck.map(r => {
      const filed = r.filed_date ? new Date(r.filed_date).toISOString().slice(0,10) : '-';
      const years = r.days_waiting ? (r.days_waiting / 365).toFixed(1) + 'y' : '';
      return [
        r.permit_number ?? '',
        filed,
        `<span class="badge ${r.status === 'Filed' ? 'badge-red' : 'bg-amber-50 text-amber-700 ring-1 ring-amber-600/20'} text-xs">${r.status ?? ''}</span>`,
        `${comma(r.days_waiting)} <span class="text-gray-400 text-xs">(${years})</span>`,
        r.district ?? '',
        `<span class="text-xs">${r.neighborhood ?? ''}</span>`,
        r.units ?? '',
        `<span class="text-xs text-gray-500">${(r.description ?? '').slice(0, 80)}</span>`,
      ];
    });
    document.getElementById('stuck-table').innerHTML = makeTable(['Permit','Filed','Status','Days Waiting','District','Neighborhood','Units','Description'], rows);
  }
}

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
