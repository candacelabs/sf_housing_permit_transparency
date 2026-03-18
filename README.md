# SF Housing Permit Transparency

Making San Francisco's housing permitting pipeline transparent. 1.28M permits analyzed, 9,500+ stuck in the pipeline blocking 104,000+ housing units.

## Why this exists

SF has ~50,000 housing units approved but stuck in permitting. The system is fragmented across DBI, Planning, and PermitSF — opaque and slow. Mayor Lurie announced consolidation of these agencies by mid-2027. This tool makes the current mess visible so policymakers can act on it.

## What it does

- Downloads 1.28M building permits from [SF Open Data](https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9)
- Computes processing times across every stage: filed, approved, issued, construction, completed
- Identifies stuck permits (filed >1 year, never issued)
- Ranks Supervisor Districts by bottleneck severity
- Tracks trends over time with policy milestone annotations (COVID, AB 2011, SB 423, etc.)
- Interactive Dash dashboard with drill-down filtering
- HTML policy brief generator for policymakers

## Quickstart

```bash
# Install dependencies
uv sync

# Run full pipeline: fetch data, clean, generate report, launch dashboard
just pipeline

# Or step by step:
just fetch       # download from DataSF (~300MB, takes a few minutes)
just clean       # parse dates, compute durations, flag housing permits
just dashboard   # launch at http://127.0.0.1:8050
```

## Commands

| Command | What it does |
|---------|-------------|
| `just fetch` | Download datasets from DataSF (cached 24h) |
| `just clean` | Clean and process raw data |
| `just analyze` | Print analysis summary to terminal |
| `just report` | Generate HTML policy brief in `reports/` |
| `just dashboard` | Launch interactive dashboard at :8050 |
| `just pipeline` | Run everything end-to-end |
| `just test` | Run 66 unit tests |
| `just typecheck` | Run mypy |

## Dashboard

Four interactive tabs, all with dynamic filtering by district, year range, and permit type:

- **Bottlenecks** — Where permits get stuck, by stage, district, and permit type
- **Trends** — Quarterly processing times with policy milestone annotations
- **District Scorecard** — Supervisor district rankings with composite bottleneck scores
- **Stuck Permits** — Permits filed >1 year ago that still haven't been issued, with unit counts

## Key findings (from 1.28M permits)

- **9,503 permits** are stuck in the pipeline, blocking **104,784 housing units**
- **District 6** (SoMa/Tenderloin) has the most units stuck: 42,180
- Some permits have been "approved" since **1981** and never issued
- Median time from filing to issuance varies wildly by district and permit type

## Data sources

| Dataset | Records | Source |
|---------|---------|--------|
| DBI Building Permits | 1,284,538 | [data.sfgov.org](https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9) |
| MOHCD Affordable Housing Pipeline | 194 | [data.sfgov.org](https://data.sfgov.org/Housing-and-Buildings/Mayor-s-Office-of-Housing-and-Community-Developmen/aaxw-2cb8) |

## Production deployment

```bash
# First time: fetch + clean data locally
just fetch       # downloads ~300MB from DataSF
just clean       # parses and saves to data/processed/

# Start the container (mounts your local data/)
docker compose up --build -d

# Dashboard at http://your-server:8050
```

To refresh data later:
```bash
just fetch && just clean
docker compose restart
```

Logs:
```bash
docker compose logs -f
```

The container auto-restarts on crash, debug/reload is off, and it binds to `0.0.0.0`.

## Project structure

```
src/
  config.py                  # Dataset IDs, policy milestones, settings
  ingestion/
    fetch.py                 # CSV bulk download with caching
    clean.py                 # Date parsing, duration computation, housing flags
  analysis/
    bottlenecks.py           # Stage durations, stuck permits, district scorecards
    trends.py                # Quarterly/annual trends, policy impact analysis
  dashboard/
    app.py                   # Interactive Plotly Dash app
  reports/
    generate.py              # HTML policy brief generator
tests/                       # 66 pytest unit tests
```

## Policy context

- **AB 2011** (2023): Ministerial approval for housing on commercial land
- **SB 423** (2023): Extended streamlined approval process
- **Builder's Remedy** (2024): Allows projects when cities lack compliant housing elements
- **Lurie consolidation** (2026): Merging DBI + Planning + PermitSF into one agency (Nov 2026 ballot measure)

## License

Public domain. This is civic tech — use it however you want.
