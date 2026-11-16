"""Offline tests: no network. They build a tiny fake lake and run the real code on it."""

import json
from datetime import date

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


def fake_open_meteo(times, temps) -> bytes:
    n = len(times)
    return json.dumps(
        {
            "latitude": 45.46,
            "longitude": 9.19,
            "hourly": {
                "time": times,
                "temperature_2m": temps,
                "wind_speed_100m": [10.0] * n,
                "shortwave_radiation": [0.0] * n,
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


def test_transform_and_export_end_to_end(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    nord, sud = ZONES[0], ZONES[3]
    lake.put_bytes(
        open_meteo.raw_key(nord, date(2026, 10, 1)),
        # last value is null: not yet published by the archive -> must be dropped
        fake_open_meteo(["2026-10-01T00:00", "2026-10-01T01:00", "2026-10-01T02:00"], [12.5, 11.0, None]),
    )
    lake.put_bytes(open_meteo.raw_key(sud, date(2026, 10, 1)), fake_open_meteo(["2026-10-01T00:00"], [18.0]))

    counts = transform.run(lake)
    assert counts["weather_hourly"] == 3  # 2 NORD + 1 SUD, null row dropped

    manifest = export.run(settings, lake)
    daily = pa.ipc.open_file(settings.site_data_dir / "weather_daily.arrow").read_all()
    assert daily.num_rows == 2  # one day for each zone
    assert set(daily.column("zone").to_pylist()) == {"NORD", "SUD"}
    assert manifest["datasets"]["weather_daily"]["licence"] == "CC BY 4.0"


def test_quality_check_rejects_impossible_temperature(tmp_path):
    settings = make_settings(tmp_path)
    lake = Lake(settings)
    lake.put_bytes(open_meteo.raw_key(ZONES[0], date(2026, 10, 1)), fake_open_meteo(["2026-10-01T00:00"], [999.0]))
    with pytest.raises(ValueError, match="temperature"):
        transform.run(lake)
