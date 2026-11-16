"""Step 3 (export): write the files the public dashboard reads.

This is the *only* place where data leaves the lake for the public website, so the
licensing rules live here. Each export declares whether it may be published:

    public=True   the data's licence allows republishing (e.g. CC BY 4.0) -> exported
    public=False  e.g. ENTSO-E day-ahead prices, not on ENTSO-E's free re-use list
                  -> stays in the lake; only results *derived* from it get exported

Output format: Apache Arrow IPC files (.arrow). Arrow is a columnar binary format that
Perspective loads directly in the browser, with column types (dates, floats) preserved;
a CSV would need to be parsed and its types guessed.

A small manifest.json records when the data was built, the row counts and the
attribution text, so the page can show them.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc

from .config import Settings
from .lake import Lake
from .transform import processed_key

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Export:
    name: str
    sql: str  # reads the processed table as `src`
    source_table: str
    public: bool
    licence: str
    attribution: str


OPEN_METEO = dict(
    licence="CC BY 4.0",
    attribution="Weather data by Open-Meteo.com (CC BY 4.0), aggregated by Energy Lab",
)
ENTSOE_CC_BY = dict(
    licence="CC BY 4.0",
    attribution="Load forecast: ENTSO-E Transparency Platform (CC BY 4.0), aggregated by Energy Lab",
)

EXPORTS = [
    # Full history at daily resolution: small, good for long-run charts.
    Export(
        name="weather_daily",
        source_table="weather_hourly",
        public=True,
        sql="""
            SELECT
                zone,
                CAST(ts_utc AS DATE)                          AS day,
                CAST(date_trunc('month', ts_utc) AS DATE)     AS month,
                CAST(avg(temperature_c) AS FLOAT)             AS temp_mean_c,
                CAST(max(temperature_c) AS FLOAT)             AS temp_max_c,
                CAST(avg(wind_speed_100m_kmh) AS FLOAT)       AS wind_100m_mean_kmh,
                -- mean W/m2 over each hour, summed over the day, /1000 -> kWh per m2 per day
                CAST(sum(solar_radiation_wm2) / 1000 AS FLOAT) AS solar_kwh_m2
            FROM src
            GROUP BY ALL
            ORDER BY zone, day
        """,
        **OPEN_METEO,
    ),
    # Recent hours at full resolution: for the hour-by-hour explorer.
    Export(
        name="weather_hourly_recent",
        source_table="weather_hourly",
        public=True,
        sql="""
            SELECT *, CAST(hour(ts_utc) AS INTEGER) AS hour_utc
            FROM src
            WHERE ts_utc >= (SELECT max(ts_utc) FROM src) - INTERVAL 60 DAY
            ORDER BY zone, ts_utc
        """,
        **OPEN_METEO,
    ),
    # ENTSO-E day-ahead load forecast (6.1.B): on ENTSO-E's free re-use list -> public.
    Export(
        name="load_forecast_daily",
        source_table="load_forecast_hourly",
        public=True,
        sql="""
            SELECT
                zone,
                CAST(ts_utc AS DATE)                      AS day,
                CAST(date_trunc('month', ts_utc) AS DATE) AS month,
                CAST(avg(value) AS FLOAT)                 AS load_fc_mean_mw,
                CAST(max(value) AS FLOAT)                 AS load_fc_peak_mw,
                CAST(sum(value) / 1000 AS FLOAT)          AS load_fc_energy_gwh
            FROM src
            GROUP BY ALL
            ORDER BY zone, day
        """,
        **ENTSOE_CC_BY,
    ),
    Export(
        name="load_forecast_hourly_recent",
        source_table="load_forecast_hourly",
        public=True,
        sql="""
            WITH recent AS (
                SELECT * FROM src
                WHERE ts_utc >= (SELECT max(ts_utc) FROM src) - INTERVAL 60 DAY
            )
            SELECT
                zone,
                ts_utc,
                CAST(value AS FLOAT)            AS load_fc_mw,
                -- each zone relative to its own average, so zones of very different
                -- size (NORD is ~25x CALA) can be compared on one colour scale
                CAST(100 * value / avg(value) OVER (PARTITION BY zone) AS FLOAT) AS load_fc_pct_of_zone_avg,
                CAST(hour(ts_utc) AS INTEGER)   AS hour_utc
            FROM recent
            ORDER BY zone, ts_utc
        """,
        **ENTSOE_CC_BY,
    ),
    # Day-ahead prices (12.1.D) and actual load (6.1.A) are NOT on the free re-use list.
    # Declared here so the rule is visible; the export loop skips them.
    Export(
        name="prices_daily",
        source_table="prices_hourly",
        public=False,
        sql="SELECT zone, CAST(ts_utc AS DATE) AS day, avg(value) AS price_eur_mwh FROM src GROUP BY ALL",
        licence="ENTSO-E / Terna, not openly licensed",
        attribution="",
    ),
]


def _write_arrow(table: pa.Table, path) -> None:
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)


def run(settings: Settings, lake: Lake) -> dict:
    out_dir = settings.site_data_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {"generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "datasets": {}}

    for exp in EXPORTS:
        if not exp.public:
            log.info("Skipping %s: not licensed for republication", exp.name)
            continue
        if not lake.exists(processed_key(exp.source_table)):
            log.warning("Skipping %s: %s has not been built", exp.name, exp.source_table)
            continue
        con = lake.duckdb()
        lake.view(con, "src", processed_key(exp.source_table))
        result = con.sql(exp.sql)
        # DuckDB 2.x renamed fetch_arrow_table() to to_arrow_table(); support both
        table = result.to_arrow_table() if hasattr(result, "to_arrow_table") else result.fetch_arrow_table()
        _write_arrow(table, out_dir / f"{exp.name}.arrow")
        time_col = next((c for c in ("day", "ts_utc") if c in table.column_names), None)
        manifest["datasets"][exp.name] = {
            "file": f"{exp.name}.arrow",
            "rows": table.num_rows,
            "first": str(pc.min(table[time_col])) if time_col else None,
            "last": str(pc.max(table[time_col])) if time_col else None,
            "licence": exp.licence,
            "attribution": exp.attribution,
        }
        log.info("Exported %s: %d rows", exp.name, table.num_rows)

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
