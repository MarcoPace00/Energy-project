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
import os
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
    # extra processed tables the SQL needs, each available under its own name
    joins: tuple[str, ...] = ()


OPEN_METEO = dict(
    licence="CC BY 4.0",
    attribution="Weather data by Open-Meteo.com (CC BY 4.0), aggregated by Energy Lab",
)
ENTSOE_CC_BY = dict(
    licence="CC BY 4.0",
    attribution="Load forecast: ENTSO-E Transparency Platform (CC BY 4.0), aggregated by Energy Lab",
)

# Time convention (the usual one in European power markets):
#   - the lake stores UTC: no missing or repeated hours, safe to join on;
#   - days and hours shown to people are Italian delivery days/hours (Europe/Rome), as in
#     the day-ahead market. The day the clocks go forward has 23 hours and the day they go
#     back has 25; they are kept as they are (daily energy is lower/higher on those days).
LOCAL_TZ = "Europe/Rome"
# UTC timestamp -> Italian wall-clock time (timezone() needs DuckDB's ICU extension)
TS_LOCAL = f"timezone('{LOCAL_TZ}', timezone('UTC', ts_utc))"
# 23, 24 or 25: hours between this Italian midnight and the next one
HOURS_IN_LOCAL_DAY = (
    f"date_diff('hour', timezone('{LOCAL_TZ}', CAST(day AS TIMESTAMP)),"
    f" timezone('{LOCAL_TZ}', CAST(day + INTERVAL 1 DAY AS TIMESTAMP)))"
)


def _daily_sql(table: str, measures: str) -> str:
    """Aggregate an hourly table to Italian days, keeping only days with every hour.

    Incomplete days (the start of the history, a day not fully published yet, a gap)
    would give wrong daily totals, so they are left out.
    """
    return f"""
    WITH hours AS (SELECT *, CAST({TS_LOCAL} AS DATE) AS day FROM {table}),
    days AS (
        SELECT zone, day, CAST(date_trunc('month', day) AS DATE) AS month,
               {measures}
               , count(*) AS n_hours  -- own line: measures may end with a -- comment
        FROM hours
        GROUP BY zone, day
    )
    SELECT * EXCLUDE (n_hours) FROM days WHERE n_hours = {HOURS_IN_LOCAL_DAY}
    """


def _incomplete_days_sql(table: str) -> str:
    """Every Italian day that is not complete: some hours missing, or none at all.

    is_gap = the day lies between the zone's first and last complete days, i.e. a hole
    in the history (unexpected). Otherwise it is an edge day (expected): the start of
    the history, or recent days not fully published yet.
    """
    return f"""
    WITH hours AS (SELECT zone, CAST({TS_LOCAL} AS DATE) AS day FROM {table}),
    counted AS (SELECT zone, day, count(*) AS n_hours FROM hours GROUP BY zone, day),
    bounds AS (
        SELECT zone,
               min(day) FILTER (WHERE n_hours = {HOURS_IN_LOCAL_DAY}) AS first_ok,
               max(day) FILTER (WHERE n_hours = {HOURS_IN_LOCAL_DAY}) AS last_ok
        FROM counted GROUP BY zone
    ),
    calendar AS (  -- every day between the first and last complete day, to catch empty days
        SELECT zone, CAST(unnest(generate_series(first_ok, last_ok, INTERVAL 1 DAY)) AS DATE) AS day
        FROM bounds WHERE first_ok IS NOT NULL
    ),
    all_days AS (
        SELECT zone, day, coalesce(c.n_hours, 0) AS n_hours
        FROM counted c FULL JOIN calendar k USING (zone, day)
    )
    SELECT zone, day, n_hours, {HOURS_IN_LOCAL_DAY} AS expected,
           coalesce(day > first_ok AND day < last_ok, false) AS is_gap
    FROM all_days JOIN bounds USING (zone)
    WHERE n_hours <> {HOURS_IN_LOCAL_DAY}
    ORDER BY zone, day
    """


# Hourly tables that feed daily exports: their incomplete days are reported.
DAILY_SOURCES = ("weather_hourly", "load_forecast_hourly")


def report_incomplete_days(lake: Lake) -> dict:
    """Find, log and return the days left out of the daily tables.

    Nothing is deleted from the lake: the daily tables are rebuilt from the hourly data
    on every run, so a day reappears as soon as its missing hours arrive (the fetch
    re-downloads the last two months every day). Gaps older than that need a
    `--full-refresh` of the source; they are raised as warnings, which GitHub Actions
    also shows in the run summary.
    """
    report = {}
    for table in DAILY_SOURCES:
        if not lake.exists(processed_key(table)):
            continue
        con = lake.duckdb()
        lake.view(con, "src", processed_key(table))
        rows = con.sql(_incomplete_days_sql("src")).fetchall()
        fmt = [(f"{zone} {day} ({n}/{exp} h)", gap) for zone, day, n, exp, gap in rows]
        edges = [s for s, gap in fmt if not gap]
        gaps = [s for s, gap in fmt if gap]
        if edges:
            log.info("%s: %d incomplete day(s) at the edges left out: %s", table, len(edges), ", ".join(edges))
        for s in gaps:
            msg = f"{table}: gap in the history, day left out: {s}"
            log.warning(msg)
            if os.environ.get("GITHUB_ACTIONS") == "true":
                print(f"::warning title=Data gap::{msg}", flush=True)
        report[table] = {"edge_days_left_out": edges, "gaps_left_out": gaps}
    return report


def weather_daily_sql(table: str) -> str:
    return _daily_sql(table, """
        CAST(avg(temperature_c) AS FLOAT)             AS temp_mean_c,
        CAST(max(temperature_c) AS FLOAT)             AS temp_max_c,
        CAST(avg(wind_speed_100m_kmh) AS FLOAT)       AS wind_100m_mean_kmh,
        -- mean W/m2 over each hour, summed over the day, /1000 -> kWh per m2 per day
        CAST(sum(solar_radiation_wm2) / 1000 AS FLOAT) AS solar_kwh_m2""")


def load_fc_daily_sql(table: str) -> str:
    return _daily_sql(table, """
        CAST(avg(value) AS FLOAT)        AS load_fc_mean_mw,    -- average power over the day
        CAST(max(value) AS FLOAT)        AS load_fc_peak_mw,    -- highest hour
        CAST(sum(value) / 1000 AS FLOAT) AS load_fc_energy_gwh  -- each hour: MW x 1 h = MWh""")


EXPORTS = [
    # Full history at daily resolution: small, good for long-run charts.
    Export(
        name="weather_daily",
        source_table="weather_hourly",
        public=True,
        sql=weather_daily_sql("src") + "ORDER BY zone, day",
        **OPEN_METEO,
    ),
    # Recent hours at full resolution: for the hour-by-hour explorer.
    Export(
        name="weather_hourly_recent",
        source_table="weather_hourly",
        public=True,
        sql=f"""
            SELECT *, CAST(hour({TS_LOCAL}) AS INTEGER) AS hour_local
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
        sql=load_fc_daily_sql("src") + "ORDER BY zone, day",
        **ENTSOE_CC_BY,
    ),
    # Weather and load forecast side by side, one row per zone and day: the explorer table.
    # Inner join: only days with both. The load forecast already covers tomorrow while the
    # weather stops at yesterday; those newest days are left out rather than shown half empty.
    Export(
        name="zone_daily",
        source_table="weather_hourly",
        joins=("load_forecast_hourly",),
        public=True,
        sql=f"""
            WITH w AS ({weather_daily_sql("src")}),
                 l AS ({load_fc_daily_sql("load_forecast_hourly")})
            SELECT
                w.zone, w.day, w.month,
                w.temp_mean_c, w.temp_max_c, w.wind_100m_mean_kmh, w.solar_kwh_m2,
                l.load_fc_mean_mw, l.load_fc_peak_mw, l.load_fc_energy_gwh
            FROM w JOIN l ON w.zone = l.zone AND w.day = l.day
            ORDER BY zone, day
        """,
        licence="CC BY 4.0",
        attribution=OPEN_METEO["attribution"] + " · " + ENTSOE_CC_BY["attribution"],
    ),
    Export(
        name="load_forecast_hourly_recent",
        source_table="load_forecast_hourly",
        public=True,
        sql=f"""
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
                CAST(hour({TS_LOCAL}) AS INTEGER) AS hour_local
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
        sql=_daily_sql("src", "avg(value) AS price_eur_mwh"),
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
        missing = [t for t in (exp.source_table, *exp.joins) if not lake.exists(processed_key(t))]
        if missing:
            log.warning("Skipping %s: %s not built", exp.name, ", ".join(missing))
            continue
        con = lake.duckdb()
        lake.view(con, "src", processed_key(exp.source_table))
        for t in exp.joins:
            lake.view(con, t, processed_key(t))
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

    manifest["incomplete_days_left_out"] = report_incomplete_days(lake)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
