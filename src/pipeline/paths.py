"""Lake path helpers. Bronze and silver are partitioned by ingest_date=YYYY-MM-DD."""

from __future__ import annotations

from datetime import date
from pathlib import Path


def partition_dir(root: Path, layer: str, source: str, ingest_date: date) -> Path:
    return root / layer / source / f"ingest_date={ingest_date.isoformat()}"


def bronze_payload_path(data_dir: Path, source: str, ingest_date: date) -> Path:
    return partition_dir(data_dir, "bronze", source, ingest_date) / "payload.json"


def bronze_manifest_path(data_dir: Path, source: str, ingest_date: date) -> Path:
    return partition_dir(data_dir, "bronze", source, ingest_date) / "_manifest.json"


def silver_parquet_path(data_dir: Path, source: str, ingest_date: date) -> Path:
    return partition_dir(data_dir, "silver", source, ingest_date) / f"{source}.parquet"
