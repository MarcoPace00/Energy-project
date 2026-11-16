# Italian Power Market Lab

A data pipeline and interactive dashboard for the Italian power market, updated daily.

**Live dashboard: https://marcopace00.github.io/Energy-project/**

**Goal of the project:** estimate how much a battery in each Italian bidding zone can earn,
how much of that is lost by trading on forecasts instead of known prices, and what the
risk of a small energy book looks like. Built in stages:

| Stage | Status |
|---|---|
| 1. Data pipeline (weather by bidding zone) + dashboard | ✅ running |
| 1b. ENTSO-E power market data: day-ahead prices, actual and forecast load | ✅ running |
| 1c. ENTSO-E generation data (wind/solar forecasts, actual generation) | planned |
| 2. Day-ahead price forecasts, walk-forward, vs the LEAR benchmark | planned |
| 3. Battery storage optimiser | planned |
| 4. Risk engine (VaR, expected shortfall, stress tests) | planned |

## Architecture

```
Open-Meteo (JSON) ─┐
                   ├─► raw/ (untouched) ──► DuckDB SQL + quality checks ──► processed/ (Parquet)
ENTSO-E (XML) ─────┘         (XML is parsed in Python first)
                                                                              │
                                     licence gate: only publishable data ◄────┘
                                                   │
                                                   ▼
                                  site/data/*.arrow ──► Perspective dashboard (in the browser)
```

The same Python code runs in two modes:

| | Public mode | Local stack |
|---|---|---|
| Scheduler | GitHub Actions (daily cron) | Airflow |
| Data lake | folder, kept between runs by the Actions cache | MinIO (S3-compatible object storage) |
| Website | GitHub Pages | local web server |
| Start | automatic | `docker compose up --build` |

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest -q
python -m energy_lab.pipeline all                     # first run backfills from 2023
python -m http.server -d site 8000                    # http://localhost:8000
```

Local stack: copy `.env.example` to `.env`, set a MinIO password, then `docker compose up --build`.
Airflow is at http://localhost:8080, the MinIO console at http://localhost:9001 and the
dashboard at http://localhost:8000.

## Data and licences

- **Weather:** [Open-Meteo](https://open-meteo.com/) historical weather API, CC BY 4.0,
  aggregated by this project.
- **Power market data:** [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/),
  data provided by Terna. Only datasets on ENTSO-E's list of data available for free
  re-use (CC BY 4.0) are republished, currently the day-ahead load forecast. Other
  datasets, such as day-ahead prices and actual load, are used for modelling, and only
  derived results are published.
- No raw third-party data is committed to this repository; the pipeline downloads it.

Every dataset, its licence and the permission requests are listed in
[docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

Running the ENTSO-E part needs a free API token: put it in `.env` locally
(`ENTSOE_API_TOKEN=...`) and in the GitHub secret `ENTSOE_API_TOKEN`. Without a token,
the pipeline skips ENTSO-E and runs on weather data only.

Research project, not investment advice. Data may contain errors or gaps.
