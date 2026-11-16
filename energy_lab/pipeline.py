"""The pipeline's entry points.

Both "orchestrators" call exactly these functions, so the logic is written once:

    GitHub Actions   python -m energy_lab.pipeline all
    Airflow          one task per step_* function (dags/energy_daily.py)

Usage:
    python -m energy_lab.pipeline all                       # fetch + transform + export
    python -m energy_lab.pipeline fetch                     # every source
    python -m energy_lab.pipeline fetch --source entsoe     # one source
    python -m energy_lab.pipeline fetch --full-refresh      # re-download everything
    python -m energy_lab.pipeline transform
    python -m energy_lab.pipeline export
"""

from __future__ import annotations

import argparse
import logging

from . import export, transform
from .config import load_settings
from .ingest import entsoe, open_meteo
from .lake import Lake

SOURCES = {"open_meteo": open_meteo, "entsoe": entsoe}


def _fetch(source: str, full_refresh: bool) -> int:
    settings = load_settings()
    return SOURCES[source].run(settings, Lake(settings), full_refresh=full_refresh)


def step_fetch_open_meteo(full_refresh: bool = False) -> int:
    return _fetch("open_meteo", full_refresh)


def step_fetch_entsoe(full_refresh: bool = False) -> int:
    return _fetch("entsoe", full_refresh)


def step_transform() -> dict:
    return transform.run(Lake(load_settings()))


def step_export() -> dict:
    settings = load_settings()
    return export.run(settings, Lake(settings))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="energy_lab.pipeline", description=__doc__.split("\n\n")[0])
    parser.add_argument("step", choices=["fetch", "transform", "export", "all"])
    parser.add_argument("--source", choices=list(SOURCES), help="fetch only this source")
    parser.add_argument("--full-refresh", action="store_true", help="re-download all raw data")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if args.step in ("fetch", "all"):
        for source in [args.source] if args.source else SOURCES:
            _fetch(source, args.full_refresh)
    if args.step in ("transform", "all"):
        step_transform()
    if args.step in ("export", "all"):
        step_export()


if __name__ == "__main__":
    main()
