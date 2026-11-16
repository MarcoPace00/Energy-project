"""Step 1 (extract): download Italian power-market data from the ENTSO-E Transparency Platform.

Datasets (codes are the Transparency Regulation articles shown on the platform):

    prices         12.1.D  day-ahead prices          public=False (not on the free re-use list)
    load_actual    6.1.A   actual total load         public=False, revised for ~40 days
    load_forecast  6.1.B   day-ahead load forecast   public=True  (CC BY 4.0)

Lake layout: one XML file per dataset, zone and month, stored exactly as received:

    raw/entsoe/dataset=prices/zone=NORD/month=2026-10/data.xml

Same incremental rule as Open-Meteo: download months that are missing, and always the last
REFRESH_MONTHS months (actual load is revised for ~40 days, prices can be republished).

API rules: the token goes in the `securityToken` parameter; at most 400 requests per
minute, or the token is blocked for 10 minutes. Each answer takes ~20 s whatever its size
(measured Nov 2026), so time is spent waiting, not transferring: several downloads run in
parallel. With 4 workers we measured ~11 requests/minute, far below the limit; 8 workers
made ENTSO-E's gateway time out (HTTP 599). Full backfill (~990 requests) ~90 minutes;
a daily run (~42 requests) ~4 minutes.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests

from ..config import ZONES, Settings, Zone
from ..lake import Lake
from .entsoe_xml import acknowledgement_reason, is_acknowledgement
from .open_meteo import _session, month_starts

log = logging.getLogger(__name__)

API_URL = "https://web-api.tp.entsoe.eu/api"
REFRESH_MONTHS = 2
MAX_WORKERS = 4  # 8 triggered gateway timeouts (HTTP 599) on ENTSO-E's side


@dataclass(frozen=True)
class Dataset:
    name: str
    value_tag: str  # XML element holding the number
    params: dict  # request parameters; "{eic}" is replaced by the zone's code
    zone_param_keys: tuple[str, ...]


DATASETS = {
    "prices": Dataset(
        name="prices",
        value_tag="price.amount",
        params={"documentType": "A44"},
        zone_param_keys=("in_Domain", "out_Domain"),
    ),
    "load_actual": Dataset(
        name="load_actual",
        value_tag="quantity",
        params={"documentType": "A65", "processType": "A16"},
        zone_param_keys=("outBiddingZone_Domain",),
    ),
    "load_forecast": Dataset(
        name="load_forecast",
        value_tag="quantity",
        params={"documentType": "A65", "processType": "A01"},
        zone_param_keys=("outBiddingZone_Domain",),
    ),
}


def raw_key(dataset: str, zone: Zone, month: date) -> str:
    return f"raw/entsoe/dataset={dataset}/zone={zone.code}/month={month:%Y-%m}/data.xml"


def request_window(month: date, today: date) -> tuple[datetime, datetime]:
    """UTC start/end for one month, capped at the end of tomorrow (day-ahead data)."""
    start = datetime(month.year, month.month, 1, tzinfo=timezone.utc)
    nxt = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = datetime(nxt.year, nxt.month, 1, tzinfo=timezone.utc)
    cap = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) + timedelta(days=2)
    return start, min(end, cap)


def work_list(lake: Lake, start: date, today: date, full_refresh: bool) -> list[tuple[str, Zone, date]]:
    months = month_starts(start, today)
    recent = set(months[-REFRESH_MONTHS:])
    return [
        (ds, zone, m)
        for ds in DATASETS
        for m in months
        for zone in ZONES
        if full_refresh or m in recent or not lake.exists(raw_key(ds, zone, m))
    ]


def fetch_one(session: requests.Session, token: str, dataset: str, zone: Zone, month: date, today: date) -> bytes:
    ds = DATASETS[dataset]
    start, end = request_window(month, today)
    params = {
        "securityToken": token,
        **ds.params,
        **{k: zone.eic for k in ds.zone_param_keys},
        "periodStart": start.strftime("%Y%m%d%H%M"),
        "periodEnd": end.strftime("%Y%m%d%H%M"),
    }
    resp = session.get(API_URL, params=params, timeout=180)
    # "No data" comes back as HTTP 200 with an Acknowledgement document; a real problem
    # (bad parameter, bad token) as HTTP 4xx with an Acknowledgement explaining why.
    if resp.status_code >= 400:
        reason = acknowledgement_reason(resp.content) if resp.content.lstrip().startswith(b"<") else resp.text[:200]
        raise RuntimeError(f"ENTSO-E {dataset}/{zone.code}/{month:%Y-%m}: HTTP {resp.status_code}: {reason}")
    return resp.content


def run(settings: Settings, lake: Lake, today: date | None = None, full_refresh: bool = False) -> int:
    if not settings.entsoe_token:
        log.warning("ENTSOE_API_TOKEN not set: skipping the ENTSO-E source")
        return 0
    today = today or date.today()
    todo = work_list(lake, settings.history_start, today, full_refresh)
    log.info("ENTSO-E: %d dataset-zone-months to download", len(todo))

    session = _session()
    done, failed = 0, []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_one, session, settings.entsoe_token, ds, zone, m, today): (ds, zone, m)
            for ds, zone, m in todo
        }
        for fut in as_completed(futures):
            ds, zone, m = futures[fut]
            label = f"{ds}/{zone.code}/{m:%Y-%m}"
            try:
                content = fut.result()
            except Exception as exc:  # keep going: save every other download first
                log.error("  failed: %s: %s", label, exc)
                failed.append(label)
                continue
            if is_acknowledgement(content):
                log.info("  no data: %s (%s)", label, acknowledgement_reason(content))
            lake.put_bytes(raw_key(ds, zone, m), content)
            done += 1
            if done % 25 == 0 or done + len(failed) == len(todo):
                log.info("  %d/%d downloaded", done, len(todo))
    if failed:
        # Fail the run visibly. The next run retries exactly these, because their files are missing.
        raise RuntimeError(f"ENTSO-E: {len(failed)} downloads failed, e.g. {failed[:5]}")
    return done
