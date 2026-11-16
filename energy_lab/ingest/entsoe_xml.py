"""Parse ENTSO-E XML documents into rows: (ts_utc, resolution_min, value).

DuckDB cannot read XML, so this small parser turns the raw files into a table that
DuckDB then cleans with SQL (see transform.py). The raw XML stays untouched in the lake.

Structure of an ENTSO-E data document (namespaces omitted):

    <Publication_MarketDocument>          (prices; load uses GL_MarketDocument)
      <TimeSeries>                         one or more per document
        <curveType>A03</curveType>
        <Period>                           usually one per day
          <timeInterval><start>2026-11-14T23:00Z</start><end>...</end></timeInterval>
          <resolution>PT15M</resolution>   PT60M until 2024, PT15M from 1 Jan 2025
          <Point><position>1</position><price.amount>265.97</price.amount></Point>
          ...

Two traps handled here:

1. **Omitted points (curve type A03).** If a value is the same as the previous one,
   ENTSO-E leaves the point out to save space: a day with 96 quarter-hours can arrive
   with 94 points. Position p always means "interval p of the period", so we rebuild
   every interval and carry the last value forward. Reading the points in order without
   this step would shift every later value to the wrong time, with no error.
2. **Acknowledgement documents.** When there is no data (e.g. a future date), the API
   returns an <Acknowledgement_MarketDocument> with a reason instead of data. That is
   not an error: it parses to zero rows.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

RESOLUTION_MINUTES = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "P1D": 1440}


def _local(tag: str) -> str:
    """'{urn:iec62325...}Period' -> 'Period' (drop the XML namespace)."""
    return tag.rsplit("}", 1)[-1]


def _child(el: ET.Element, name: str) -> ET.Element | None:
    return next((c for c in el if _local(c.tag) == name), None)


def _text(el: ET.Element, name: str) -> str | None:
    c = _child(el, name)
    return c.text.strip() if c is not None and c.text else None


def _parse_time(s: str) -> datetime:
    # ENTSO-E writes "2026-11-14T23:00Z"
    return datetime.strptime(s, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)


def is_acknowledgement(xml_bytes: bytes) -> bool:
    return _local(ET.fromstring(xml_bytes).tag) == "Acknowledgement_MarketDocument"


def acknowledgement_reason(xml_bytes: bytes) -> str:
    root = ET.fromstring(xml_bytes)
    reason = next((el for el in root.iter() if _local(el.tag) == "Reason"), None)
    return (_text(reason, "text") or _text(reason, "code") or "") if reason is not None else ""


def parse_points(xml_bytes: bytes, value_tag: str) -> list[tuple[datetime, int, float]]:
    """All intervals in the document as (start time UTC, resolution in minutes, value).

    value_tag is "price.amount" for prices and "quantity" for load/generation.
    """
    root = ET.fromstring(xml_bytes)
    if _local(root.tag) == "Acknowledgement_MarketDocument":
        return []

    rows: list[tuple[datetime, int, float]] = []
    for series in (el for el in root if _local(el.tag) == "TimeSeries"):
        for period in (el for el in series if _local(el.tag) == "Period"):
            interval = _child(period, "timeInterval")
            start = _parse_time(_text(interval, "start"))
            end = _parse_time(_text(interval, "end"))
            res_code = _text(period, "resolution")
            if res_code not in RESOLUTION_MINUTES:
                raise ValueError(f"Unknown resolution {res_code!r}")
            minutes = RESOLUTION_MINUTES[res_code]
            n_slots = int((end - start) / timedelta(minutes=minutes))

            values = {}
            for point in (el for el in period if _local(el.tag) == "Point"):
                values[int(_text(point, "position"))] = float(_text(point, value_tag))

            # Rebuild every interval; an omitted position repeats the last known value.
            last = None
            for pos in range(1, n_slots + 1):
                last = values.get(pos, last)
                if last is not None:
                    rows.append((start + timedelta(minutes=minutes * (pos - 1)), minutes, last))
    return rows
