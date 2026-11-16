"""Offline tests for the ENTSO-E source: XML parsing, hourly aggregation, incremental logic."""

import os
from datetime import date, datetime, timezone

import duckdb
import pytest

from energy_lab import transform
from energy_lab.config import ZONES, load_dotenv
from energy_lab.ingest import entsoe
from energy_lab.ingest.entsoe_xml import parse_points
from energy_lab.lake import Lake
from test_pipeline import make_settings

NS = 'xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"'


def price_doc(periods) -> bytes:
    """periods: list of (start, end, resolution, {position: price})"""
    body = ""
    for start, end, res, points in periods:
        pts = "".join(
            f"<Point><position>{p}</position><price.amount>{v}</price.amount></Point>" for p, v in points.items()
        )
        body += (
            f"<TimeSeries><curveType>A03</curveType><Period>"
            f"<timeInterval><start>{start}</start><end>{end}</end></timeInterval>"
            f"<resolution>{res}</resolution>{pts}</Period></TimeSeries>"
        )
    return f"<Publication_MarketDocument {NS}>{body}</Publication_MarketDocument>".encode()


ACK = (
    b'<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
    b"<Reason><code>999</code><text>No matching data found</text></Reason></Acknowledgement_MarketDocument>"
)


def test_omitted_points_are_filled_forward():
    # 4 quarter-hours, position 2 and 4 omitted (= same as the previous value)
    doc = price_doc([("2026-11-14T23:00Z", "2026-11-15T00:00Z", "PT15M", {1: 100.0, 3: 80.0})])
    rows = parse_points(doc, "price.amount")
    assert [v for _, _, v in rows] == [100.0, 100.0, 80.0, 80.0]
    assert rows[1][0] == datetime(2026, 11, 14, 23, 15, tzinfo=timezone.utc)
    assert all(res == 15 for _, res, _ in rows)


def test_acknowledgement_means_no_rows():
    assert parse_points(ACK, "price.amount") == []


def test_hourly_aggregation_prefers_finest_resolution_and_clips_months(tmp_path):
    lake = Lake(make_settings(tmp_path))
    nord = ZONES[0]
    oct_doc = price_doc(
        [
            # 23:00-24:00 UTC on 31 Oct: hourly AND quarter-hourly versions of the same hour
            ("2026-10-31T23:00Z", "2026-11-01T00:00Z", "PT60M", {1: 999.0}),
            ("2026-10-31T23:00Z", "2026-11-01T00:00Z", "PT15M", {1: 10.0, 2: 20.0, 3: 30.0, 4: 40.0}),
            # the delivery day runs into November: these rows belong to the November file
            ("2026-11-01T00:00Z", "2026-11-01T02:00Z", "PT60M", {1: 555.0, 2: 555.0}),
        ]
    )
    nov_doc = price_doc([("2026-11-01T00:00Z", "2026-11-01T02:00Z", "PT60M", {1: 50.0, 2: 60.0})])
    lake.put_bytes(entsoe.raw_key("prices", nord, date(2026, 10, 1)), oct_doc)
    lake.put_bytes(entsoe.raw_key("prices", nord, date(2026, 11, 1)), nov_doc)

    counts = transform.run(lake)
    assert counts["prices_hourly"] == 3
    assert counts["load_actual_hourly"] is None  # no raw files -> skipped, not an error

    rows = duckdb.sql(
        f"SELECT ts_utc, value, resolution_min, complete FROM '{lake.uri('processed/prices_hourly.parquet')}' ORDER BY 1"
    ).fetchall()
    assert rows[0][1:] == (25.0, 15, True)  # mean of the 15-min values, the 999 hourly copy ignored
    assert [r[1] for r in rows[1:]] == [50.0, 60.0]  # from the November file, not 555


def test_quality_check_rejects_impossible_price(tmp_path):
    lake = Lake(make_settings(tmp_path))
    doc = price_doc([("2026-11-01T00:00Z", "2026-11-01T01:00Z", "PT60M", {1: 99999.0})])
    lake.put_bytes(entsoe.raw_key("prices", ZONES[0], date(2026, 11, 1)), doc)
    with pytest.raises(ValueError, match="price outside"):
        transform.run(lake)


def test_request_window_caps_at_end_of_tomorrow():
    start, end = entsoe.request_window(date(2026, 11, 1), today=date(2026, 11, 16))
    assert start == datetime(2026, 11, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 11, 18, tzinfo=timezone.utc)


def test_work_list_without_token_does_nothing(tmp_path):
    settings = make_settings(tmp_path)  # entsoe_token=None
    assert entsoe.run(settings, Lake(settings)) == 0


def test_dotenv_never_overrides_real_environment(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text('ENERGY_LAB_TEST_A="from-file"\nENERGY_LAB_TEST_B=from-file\n# comment\n')
    monkeypatch.setenv("ENERGY_LAB_TEST_A", "from-environment")
    monkeypatch.delenv("ENERGY_LAB_TEST_B", raising=False)
    load_dotenv(env)
    assert os.environ["ENERGY_LAB_TEST_A"] == "from-environment"
    assert os.environ["ENERGY_LAB_TEST_B"] == "from-file"
    monkeypatch.delenv("ENERGY_LAB_TEST_B")
