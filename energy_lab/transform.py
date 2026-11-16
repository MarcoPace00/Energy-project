"""Step 2 (transform): build the processed/ tables from raw/.

Two kinds of source:
  - Open-Meteo JSON: DuckDB reads the raw files directly (sql/weather_hourly.sql).
  - ENTSO-E XML: DuckDB can't read XML, so Python parses the files into rows first
    (ingest/entsoe_xml.py), then DuckDB SQL does the cleaning (sql/entsoe_hourly.sql).

Each table is rebuilt from *all* raw files on every run ("full rebuild"). With a few
million rows at most this takes seconds, and it makes the step idempotent: running it
twice gives the same result, so a failed run can simply be run again.

After writing, every table passes data-quality checks. A failed check stops the
pipeline before bad data can reach the dashboard. A table whose raw files don't exist
yet (e.g. no ENTSO-E token) is skipped with a warning.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from importlib.resources import files

import pyarrow as pa

from .ingest import entsoe
from .ingest.entsoe_xml import parse_points
from .lake import Lake

log = logging.getLogger(__name__)


def _sql(name: str) -> str:
    return files("energy_lab").joinpath("sql", name).read_text(encoding="utf-8")


def processed_key(table: str) -> str:
    return f"processed/{table}.parquet"


# Rules every table must satisfy. Each query returns TRUE when the rule is broken.
COMMON_CHECKS = {
    "table is empty": "SELECT count(*) = 0 FROM t",
    "duplicate (zone, hour) rows": "SELECT count(*) > count(DISTINCT (zone, ts_utc)) FROM t",
}


@dataclass(frozen=True)
class Table:
    name: str
    raw_prefix: str
    checks: dict = field(default_factory=dict)
    entsoe_dataset: str | None = None  # None -> built by its own SQL file from JSON


TABLES = [
    Table(
        name="weather_hourly",
        raw_prefix="raw/open_meteo/",
        checks={
            "temperature outside -40..55 C": "SELECT count(*) > 0 FROM t WHERE temperature_c NOT BETWEEN -40 AND 55",
            "negative solar radiation": "SELECT count(*) > 0 FROM t WHERE solar_radiation_wm2 < 0",
        },
    ),
    Table(
        name="prices_hourly",
        raw_prefix="raw/entsoe/dataset=prices/",
        entsoe_dataset="prices",
        # harmonised limits of the European day-ahead market
        checks={"price outside -500..4000 EUR/MWh": "SELECT count(*) > 0 FROM t WHERE value NOT BETWEEN -500 AND 4000"},
    ),
    Table(
        name="load_actual_hourly",
        raw_prefix="raw/entsoe/dataset=load_actual/",
        entsoe_dataset="load_actual",
        checks={"load outside 0..60000 MW": "SELECT count(*) > 0 FROM t WHERE value NOT BETWEEN 0 AND 60000"},
    ),
    Table(
        name="load_forecast_hourly",
        raw_prefix="raw/entsoe/dataset=load_forecast/",
        entsoe_dataset="load_forecast",
        checks={"load outside 0..60000 MW": "SELECT count(*) > 0 FROM t WHERE value NOT BETWEEN 0 AND 60000"},
    ),
]

_PARTITION = re.compile(r"zone=(?P<zone>[A-Z]+)/month=(?P<month>\d{4}-\d{2})/")


def entsoe_rows(lake: Lake, dataset: str) -> pa.Table:
    """Parse every raw XML file of one dataset into one Arrow table.

    Each file covers one UTC month, but ENTSO-E answers with whole delivery days, so
    the first and last day can also appear in the neighbouring month's file. Rows are
    clipped to their file's month, so every interval comes from exactly one file.
    """
    value_tag = entsoe.DATASETS[dataset].value_tag
    zones, times, resolutions, values = [], [], [], []
    for key in lake.list_keys(f"raw/entsoe/dataset={dataset}/"):
        m = _PARTITION.search(key)
        month = date.fromisoformat(m["month"] + "-01")
        lo = datetime(month.year, month.month, 1, tzinfo=timezone.utc)
        hi = datetime(month.year + (month.month == 12), month.month % 12 + 1, 1, tzinfo=timezone.utc)
        for ts, res, val in parse_points(lake.get_bytes(key), value_tag):
            if lo <= ts < hi:
                zones.append(m["zone"])
                times.append(ts.replace(tzinfo=None))  # naive = UTC by convention
                resolutions.append(res)
                values.append(val)
    return pa.table(
        {
            "zone": pa.array(zones, pa.string()),
            "ts_utc": pa.array(times, pa.timestamp("us")),
            "resolution_min": pa.array(resolutions, pa.int16()),
            "value": pa.array(values, pa.float64()),
        }
    )


def build_table(lake: Lake, table: Table) -> int | None:
    if not lake.list_keys(table.raw_prefix):
        log.warning("Skipping %s: no raw files under %s yet", table.name, table.raw_prefix)
        return None

    out_key = processed_key(table.name)
    lake.prepare_dir(out_key)
    con = lake.duckdb()
    if table.entsoe_dataset is None:
        con.execute(
            f"COPY ({_sql(table.name + '.sql')}) TO $out (FORMAT parquet, COMPRESSION zstd)",
            {"raw_glob": lake.uri(table.raw_prefix + "zone=*/month=*/data.json"), "out": lake.uri(out_key)},
        )
    else:
        con.register("intervals", entsoe_rows(lake, table.entsoe_dataset))  # Arrow table -> SQL name
        con.execute(
            f"COPY ({_sql('entsoe_hourly.sql')}) TO $out (FORMAT parquet, COMPRESSION zstd)",
            {"out": lake.uri(out_key)},
        )

    check_quality(lake, table)
    n = con.execute("SELECT count(*) FROM read_parquet($p)", {"p": lake.uri(out_key)}).fetchone()[0]
    log.info("Built %s: %d rows", out_key, n)
    return n


def check_quality(lake: Lake, table: Table) -> None:
    """Fail loudly if the table breaks a rule we rely on downstream."""
    con = lake.duckdb()
    lake.view(con, "t", processed_key(table.name))
    checks = {**COMMON_CHECKS, **table.checks}
    failed = [name for name, q in checks.items() if con.execute(q).fetchone()[0]]
    if failed:
        raise ValueError(f"Data-quality checks failed for {table.name}: {failed}")


def run(lake: Lake) -> dict[str, int | None]:
    return {t.name: build_table(lake, t) for t in TABLES}
