"""The data lake: where every file the pipeline produces is stored.

Layout (the same in both modes):

    raw/         files exactly as the source sent them; never edited
    processed/   clean, typed tables (Parquet) built from raw/ by DuckDB

The `Lake` class hides *where* the lake lives. In local mode it is a folder; in s3 mode
it is a MinIO bucket (MinIO speaks the Amazon S3 protocol, so the same code would work
on real AWS S3). The rest of the pipeline only asks the lake to:

    - store bytes under a key        lake.put_bytes("raw/...json", data)
    - check whether a key exists     lake.exists("raw/...json")
    - give a path DuckDB can read    lake.uri("processed/x.parquet")
    - give a ready DuckDB connection lake.duckdb()
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from .config import Settings


class Lake:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._s3 = None
        if settings.lake_mode == "s3":
            self._s3 = self._make_s3_client()
            self._ensure_bucket()

    # ------------------------------------------------------------------ paths
    def uri(self, key: str) -> str:
        """Full address of a key, in a form DuckDB understands (may contain * globs)."""
        if self.settings.lake_mode == "s3":
            return f"s3://{self.settings.lake_root}/{key}"
        return (Path(self.settings.lake_root) / key).as_posix()

    # ------------------------------------------------------------- read/write
    def put_bytes(self, key: str, data: bytes) -> None:
        if self._s3 is not None:
            self._s3.put_object(Bucket=self.settings.lake_root, Key=key, Body=data)
        else:
            path = Path(self.uri(key))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:
        if self._s3 is not None:
            return self._s3.get_object(Bucket=self.settings.lake_root, Key=key)["Body"].read()
        return Path(self.uri(key)).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        """All keys starting with `prefix`, sorted (e.g. every raw file of one dataset)."""
        if self._s3 is not None:
            keys, token = [], None
            while True:  # S3 returns at most 1000 keys per call: follow the pages
                kwargs = {"Bucket": self.settings.lake_root, "Prefix": prefix}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self._s3.list_objects_v2(**kwargs)
                keys += [o["Key"] for o in resp.get("Contents", [])]
                if not resp.get("IsTruncated"):
                    return sorted(keys)
                token = resp["NextContinuationToken"]
        root = Path(self.settings.lake_root)
        base = root / prefix
        folder = base if base.is_dir() else base.parent
        if not folder.exists():
            return []
        keys = (p.relative_to(root).as_posix() for p in folder.rglob("*") if p.is_file())
        return sorted(k for k in keys if k.startswith(prefix))

    def exists(self, key: str) -> bool:
        if self._s3 is not None:
            resp = self._s3.list_objects_v2(Bucket=self.settings.lake_root, Prefix=key, MaxKeys=1)
            return resp.get("KeyCount", 0) > 0
        return Path(self.uri(key)).exists()

    def prepare_dir(self, key: str) -> None:
        """DuckDB's COPY ... TO a local path needs the folder to exist (S3 has no folders)."""
        if self._s3 is None:
            Path(self.uri(key)).parent.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------- duckdb
    def duckdb(self) -> duckdb.DuckDBPyConnection:
        """An in-memory DuckDB connection that can read and write this lake.

        In s3 mode we register the MinIO credentials as a DuckDB "secret", so that SQL
        like `SELECT * FROM 's3://energy-lake/processed/x.parquet'` just works.
        """
        con = duckdb.connect()
        if self.settings.lake_mode == "s3":
            s = self.settings
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(
                f"""
                CREATE OR REPLACE SECRET lake (
                    TYPE s3,
                    KEY_ID {_sql_str(s.s3_access_key)},
                    SECRET {_sql_str(s.s3_secret_key)},
                    ENDPOINT {_sql_str(s.s3_endpoint)},
                    USE_SSL false,
                    URL_STYLE 'path'
                )
                """
            )
        return con

    def view(self, con: duckdb.DuckDBPyConnection, name: str, key: str) -> None:
        """Expose a Parquet file in the lake as a SQL view called `name`.

        (DuckDB cannot take $parameters inside CREATE VIEW, so the path is quoted instead.)
        """
        con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({_sql_str(self.uri(key))})")

    # ---------------------------------------------------------------- private
    def _make_s3_client(self):
        import boto3  # only needed in s3 mode

        s = self.settings
        if not (s.s3_endpoint and s.s3_access_key and s.s3_secret_key):
            raise ValueError("s3 mode needs S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY")
        return boto3.client(
            "s3",
            endpoint_url=f"http://{s.s3_endpoint}",
            aws_access_key_id=s.s3_access_key,
            aws_secret_access_key=s.s3_secret_key,
        )

    def _ensure_bucket(self) -> None:
        existing = {b["Name"] for b in self._s3.list_buckets().get("Buckets", [])}
        if self.settings.lake_root not in existing:
            self._s3.create_bucket(Bucket=self.settings.lake_root)


def _sql_str(value: str | None) -> str:
    """Quote a value as a SQL string literal (doubling any single quotes)."""
    return "'" + (value or "").replace("'", "''") + "'"
