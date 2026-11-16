"""Step 1 (extract): download hourly weather per bidding zone from Open-Meteo.

Why weather: solar radiation, wind and temperature drive Italian power prices (solar
pushes midday prices down, heat pushes demand up). They will be inputs to the price
forecast. Open-Meteo needs no API key, so the pipeline runs before the ENTSO-E token exists.

Data: Open-Meteo historical weather API (reanalysis), CC BY 4.0, free for non-commercial
use, < 10,000 calls/day. https://open-meteo.com/en/terms

Lake layout, one JSON file per zone and month ("Hive-style" partitions: key=value folders):

    raw/open_meteo/zone=NORD/month=2026-10/data.json

Incremental logic, so a daily run makes ~14 calls instead of ~330:
  - a month is downloaded if its file is missing (first run = full backfill),
  - the last REFRESH_MONTHS months are always downloaded again, because the archive
    publishes recent days with a delay of a few days ("late-arriving data").
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import ZONES, Settings, Zone
from ..lake import Lake

log = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = ["temperature_2m", "wind_speed_100m", "shortwave_radiation", "cloud_cover"]
REFRESH_MONTHS = 2
PAUSE_SECONDS = 0.25  # stay far below the 600 calls/minute limit during a backfill


def raw_key(zone: Zone, month: date) -> str:
    return f"raw/open_meteo/zone={zone.code}/month={month:%Y-%m}/data.json"


def month_starts(first: date, last: date) -> list[date]:
    """First day of every month from `first` to `last`, inclusive."""
    out, m = [], first.replace(day=1)
    while m <= last:
        out.append(m)
        m = (m.replace(day=28) + timedelta(days=4)).replace(day=1)  # jump to next month
    return out


def months_to_fetch(lake: Lake, start: date, today: date, full_refresh: bool) -> list[tuple[Zone, date]]:
    months = month_starts(start, today - timedelta(days=1))
    recent = set(months[-REFRESH_MONTHS:])
    return [
        (zone, m)
        for m in months
        for zone in ZONES
        if full_refresh or m in recent or not lake.exists(raw_key(zone, m))
    ]


def _session() -> requests.Session:
    """HTTP session that retries on network errors, 429 and any 5xx, with growing waits.

    Any 5xx, not just the usual 500/502/503/504: ENTSO-E's gateway also answers 599 when
    its backend is too slow, and that is just as temporary.
    """
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, *range(500, 600)])
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "energy-lab/0.1 (research pipeline)"
    return s


def fetch_month(session: requests.Session, zone: Zone, month: date, today: date) -> bytes:
    next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = min(next_month - timedelta(days=1), today - timedelta(days=1))
    params = {
        "latitude": zone.lat,
        "longitude": zone.lon,
        "start_date": month.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",  # always store UTC; convert to Italian time only for display
        "wind_speed_unit": "kmh",
    }
    resp = session.get(ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.content  # keep the bytes exactly as received: raw/ is never edited


def run(settings: Settings, lake: Lake, today: date | None = None, full_refresh: bool = False) -> int:
    today = today or date.today()
    todo = months_to_fetch(lake, settings.history_start, today, full_refresh)
    log.info("Open-Meteo: %d zone-months to download", len(todo))
    session = _session()
    for i, (zone, month) in enumerate(todo, 1):
        lake.put_bytes(raw_key(zone, month), fetch_month(session, zone, month, today))
        if i % 25 == 0 or i == len(todo):
            log.info("  %d/%d downloaded", i, len(todo))
        time.sleep(PAUSE_SECONDS)
    return len(todo)
