"""Offline tests: no network. They build a tiny fake lake and run the real code on it."""

import json
from datetime import date, datetime, timedelta

import pyarrow as pa
import pytest

from energy_lab import export, transform
from energy_lab.config import ZONES, Settings
from energy_lab.ingest import open_meteo
from energy_lab.lake import Lake


def make_settings(tmp_path) -> Settings:
    return Settings(
        lake_mode="local",
        lake_root=str(tmp_path / "lake"),
        s3_endpoint=None,
        s3_access_key=None,
        s3_secret_key=None,
        history_start=date(2026, 9, 1),
        site_data_dir=tmp_path / "site_data",
        entsoe_token=None,
    )


def fake_open_meteo(times, temps, solar=None) -> bytes:
    n = len(times)
    return json.dumps(
        {
            "latitude": 45.46,
            "longitude": 9.19,
            "hourly": {
                "time": times,
                "temperature_2m": temps,
                "wind_speed_100m": [10.0] * n,
                "shortwave_radiation": solar if solar is not None else [0.0] * n,
                "cloud_cover": [50.0] * n,
            },
        }
    ).encode()


def test_month_starts_crosses_year_end():
    assert open_meteo.month_starts(date(2025, 11, 15), date(2026, 1, 3)) == [
        date(2025, 11, 1),
        date(2025, 12, 1),
        date(2026, 1, 1),
    ]


def test_incremental_fetch_only_missing_and_recent_months(tmp_path):
    settings = make_settings(tmp_path)  # history from 2026-09
    lake = Lake(settings)
    for zone in ZONES:  # pretend September was already downloaded
        lake.put_bytes(open_meteo.raw_key(zone, date(2026, 9, 1)), b"{}")
    todo = open_meteo.months_to_fetch(lake, settings.history_start, today=date(2026, 11, 13), full_refresh=False)
    months = sorted({m for _, m in todo})
    # September exists and is not among the last 2 months -> skipped
    assert months == [date(2026, 10, 1), date(2026, 11, 1)]


def utc_hours(first: str, n: int) -> list[datetime]:
    """n consecutive hours (naive UTC) starting at `first`."""
    t0 = datetime.fromisoformat(first)
    return [t0 + timedelta(hours=i) for i in range(n)]


def write_load_forecast(lake: Lake, rows: list[tuple[str, datetime, float]]) -> None:
    """Put a processed load_forecast_hourly table straight into the lake."""
    key = transform.processed_key("load_forecast_hourly")
    lake.prepare_dir(key)
    table = pa.table({
        "zone": [r[0] for r in rows],
        "ts_utc": pa.array([r[1] for r in rows], pa.timestamp("us")),
        "value": [r[2] for r in rows],
    })
    con = lake.duckdb()
    con.register("t", table)
    con.sql(f"COPY t TO '{lake.uri(key)}' (FORMAT parquet)")


# 10 Nov 2026 in Italy (CET, UTC+1) runs from 9 Nov 23:00 UTC to 10 Nov 23:00 UTC
NOV10_ITALY = utc_hours("2026-11-09T23:00", 24)


def test_transform_and_export_end_to_end(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    nord, sud = ZONES[0], ZONES[3]
    # a full Italian day, plus two more hours: the first one completes the last solar
    # value (labelled by the end of its hour), the second one is null, i.e. not yet
    # published by the archive -> dropped by the transform
    times = [t.strftime("%Y-%m-%dT%H:%M") for t in NOV10_ITALY] + ["2026-11-10T23:00", "2026-11-11T00:00"]
    lake.put_bytes(
        open_meteo.raw_key(nord, date(2026, 11, 1)),
        fake_open_meteo(times, [10.0 + i % 5 for i in range(24)] + [9.0, None]),
    )
    # SUD only has one hour: an incomplete day, left out of the daily table
    lake.put_bytes(open_meteo.raw_key(sud, date(2026, 11, 1)), fake_open_meteo(["2026-11-10T12:00"], [18.0]))

    counts = transform.run(lake)
    # NORD: 24 hours of 10 Nov + 23:00 UTC (its solar value would come from the null
    # row); SUD's single hour has no next hour for its solar value -> dropped
    assert counts["weather_hourly"] == 25

    manifest = export.run(settings, lake)
    daily = pa.ipc.open_file(settings.site_data_dir / "weather_daily.arrow").read_all().to_pylist()
    assert [(r["zone"], r["day"], r["temp_max_c"]) for r in daily] == [("NORD", date(2026, 11, 10), 14.0)]
    assert manifest["datasets"]["weather_daily"]["licence"] == "CC BY 4.0"
    # 11 Nov in Italy has only 1 of its 24 hours: an edge day, reported but not a gap
    assert manifest["incomplete_days_left_out"]["weather_hourly"] == {
        "edge_days_left_out": ["NORD 2026-11-11 (1/24 h)"],
        "gaps_left_out": [],
    }

    hourly = pa.ipc.open_file(settings.site_data_dir / "weather_hourly_recent.arrow").read_all()
    # 9 Nov 23:00 UTC is midnight in Italy
    assert hourly.column("hour_local").to_pylist()[0] == 0


def test_daily_tables_use_italian_days_including_clock_changes(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    rows = (
        # 29 Mar 2026, clocks go forward: 23 hours (28 Mar 23:00 UTC -> 29 Mar 22:00 UTC)
        [("NORD", t, 1000.0) for t in utc_hours("2026-03-28T23:00", 23)]
        # 25 Oct 2026, clocks go back: 25 hours (24 Oct 22:00 UTC -> 25 Oct 23:00 UTC)
        + [("NORD", t, 1000.0) for t in utc_hours("2026-10-24T22:00", 25)]
        # 10 Nov 2026 with its last hour not published yet: incomplete -> left out
        + [("NORD", t, 1000.0) for t in NOV10_ITALY[:23]]
    )
    write_load_forecast(lake, rows)

    export.run(settings, lake)
    daily = pa.ipc.open_file(settings.site_data_dir / "load_forecast_daily.arrow").read_all().to_pylist()
    assert [(r["day"], r["load_fc_mean_mw"], r["load_fc_energy_gwh"]) for r in daily] == [
        (date(2026, 3, 29), 1000.0, 23.0),  # same power, one hour less energy
        (date(2026, 10, 25), 1000.0, 25.0),  # one hour more
    ]


def test_quality_check_rejects_impossible_temperature(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    lake.put_bytes(open_meteo.raw_key(ZONES[0], date(2026, 10, 1)), fake_open_meteo(["2026-10-01T00:00", "2026-10-01T01:00"], [999.0, 999.0]))
    with pytest.raises(ValueError, match="temperature"):
        transform.run(lake)


def test_zone_daily_joins_weather_and_load_forecast(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    # one hour past the day: Open-Meteo labels the last solar mean of the day 23:00 UTC
    times = [t.strftime("%Y-%m-%dT%H:%M") for t in NOV10_ITALY] + ["2026-11-10T23:00"]
    lake.put_bytes(open_meteo.raw_key(ZONES[0], date(2026, 11, 1)), fake_open_meteo(times, [14.0] * 25))
    transform.run(lake)
    # load forecast for the same day and for the next one (no weather yet -> left out)
    write_load_forecast(lake, [("NORD", t, 21000.0) for t in utc_hours("2026-11-09T23:00", 48)])

    export.run(settings, lake)
    rows = pa.ipc.open_file(settings.site_data_dir / "zone_daily.arrow").read_all().to_pylist()
    assert [(r["day"], r["temp_max_c"], r["load_fc_mean_mw"]) for r in rows] == [
        (date(2026, 11, 10), 14.0, 21000.0),
    ]


def test_solar_radiation_is_moved_to_interval_beginning(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    # Open-Meteo: the value at 12:00 is the mean over 11:00-12:00
    lake.put_bytes(
        open_meteo.raw_key(ZONES[0], date(2026, 11, 1)),
        fake_open_meteo(["2026-11-10T11:00", "2026-11-10T12:00", "2026-11-10T13:00"],
                        [10.0, 11.0, 12.0], solar=[300.0, 400.0, 500.0]),
    )
    transform.run(lake)
    con = lake.duckdb()
    rows = con.sql(
        f"SELECT ts_utc, temperature_c, solar_radiation_wm2 "
        f"FROM '{lake.uri(transform.processed_key('weather_hourly'))}' ORDER BY ts_utc"
    ).fetchall()
    # row 11:00 = hour 11:00-12:00: temperature read at 11:00, radiation averaged over the hour
    assert rows == [
        (datetime(2026, 11, 10, 11), 10.0, 400.0),
        (datetime(2026, 11, 10, 12), 11.0, 500.0),
    ]


def test_gap_in_the_history_is_reported(tmp_path, caplog):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    hours = utc_hours("2026-11-09T23:00", 72)  # 10, 11 and 12 Nov in Italy
    del hours[30]  # one hour of 11 Nov missing
    write_load_forecast(lake, [("NORD", t, 1000.0) for t in hours])

    manifest = export.run(settings, lake)
    days = pa.ipc.open_file(settings.site_data_dir / "load_forecast_daily.arrow").read_all()
    assert days.column("day").to_pylist() == [date(2026, 11, 10), date(2026, 11, 12)]
    assert manifest["incomplete_days_left_out"]["load_forecast_hourly"] == {
        "edge_days_left_out": [],
        "gaps_left_out": ["NORD 2026-11-11 (23/24 h)"],
    }
    assert "gap in the history" in caplog.text
