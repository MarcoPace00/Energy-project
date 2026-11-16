"""All settings in one place, read from environment variables.

The same code runs in two modes, and only these variables change between them:

    public mode (GitHub Actions)    LAKE_MODE=local  -> the lake is a folder on the runner
    local stack (docker compose)    LAKE_MODE=s3     -> the lake is a MinIO bucket

Nothing else in the code should know which mode it is running in.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Zone:
    """An Italian bidding zone, with one representative weather point."""

    code: str
    eic: str  # the zone's EIC code: its identifier in ENTSO-E requests
    city: str
    lat: float
    lon: float


# Terna's mainland and island bidding zones (zoning in force since 2021).
# One city per zone is a simplification; a later version can weight several points
# by installed solar / wind capacity.
ZONES: tuple[Zone, ...] = (
    Zone("NORD", "10Y1001A1001A73I", "Milano", 45.46, 9.19),
    Zone("CNOR", "10Y1001A1001A70O", "Firenze", 43.77, 11.25),
    Zone("CSUD", "10Y1001A1001A71M", "Roma", 41.90, 12.50),
    Zone("SUD", "10Y1001A1001A788", "Bari", 41.12, 16.87),
    Zone("CALA", "10Y1001C--00096J", "Catanzaro", 38.91, 16.59),
    Zone("SICI", "10Y1001A1001A75E", "Palermo", 38.12, 13.36),
    Zone("SARD", "10Y1001A1001A74G", "Cagliari", 39.22, 9.12),
)


@dataclass(frozen=True)
class Settings:
    lake_mode: str  # "local" or "s3"
    lake_root: str  # local: a folder path; s3: a bucket name
    s3_endpoint: str | None  # e.g. "minio:9000" (host:port, no http://)
    s3_access_key: str | None
    s3_secret_key: str | None
    history_start: date  # first day the backfill goes back to
    site_data_dir: Path  # where the dashboard's public files are written
    entsoe_token: str | None  # ENTSO-E API token; without it the ENTSO-E source is skipped


def load_dotenv(path: Path = PROJECT_ROOT / ".env") -> None:
    """Read KEY=value lines from .env into the environment (local runs only).

    Variables already set (by docker compose or GitHub Actions) are never overwritten,
    so in those environments the real settings always win.
    """
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def load_settings() -> Settings:
    load_dotenv()
    mode = os.getenv("LAKE_MODE", "local").lower()
    if mode not in ("local", "s3"):
        raise ValueError(f"LAKE_MODE must be 'local' or 's3', got {mode!r}")

    default_root = str(PROJECT_ROOT / "data" / "lake") if mode == "local" else "energy-lake"

    return Settings(
        lake_mode=mode,
        lake_root=os.getenv("LAKE_ROOT", default_root),
        s3_endpoint=os.getenv("S3_ENDPOINT"),
        s3_access_key=os.getenv("S3_ACCESS_KEY"),
        s3_secret_key=os.getenv("S3_SECRET_KEY"),
        history_start=date.fromisoformat(os.getenv("HISTORY_START", "2023-01-01")),
        site_data_dir=Path(os.getenv("SITE_DATA_DIR", str(PROJECT_ROOT / "site" / "data"))),
        entsoe_token=os.getenv("ENTSOE_API_TOKEN") or None,
    )
