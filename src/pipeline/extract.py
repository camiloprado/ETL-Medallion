"""Extract: fetch BrasilAPI JSON and land it unchanged in bronze."""

from __future__ import annotations

import hashlib
import json
import logging
import socket
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from pipeline.config import Settings
from pipeline.paths import bronze_manifest_path, bronze_payload_path
from pipeline.schema import SOURCES

logger = logging.getLogger(__name__)

USER_AGENT = "br-open-data-medallion/0.1 (+https://github.com/camiloprado)"


class ExtractError(RuntimeError):
    pass


def _prefer_ipv4() -> None:
    """Prefer A records. Docker bridge IPv6 is often unroutable (Errno 101)."""
    original = socket.getaddrinfo

    def ipv4_first(
        host: str | bytes | None,
        port: str | int | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list:
        if family == 0:
            try:
                return original(host, port, socket.AF_INET, type, proto, flags)
            except OSError:
                return original(host, port, family, type, proto, flags)
        return original(host, port, family, type, proto, flags)

    socket.getaddrinfo = ipv4_first  # type: ignore[assignment]


def extract_all(settings: Settings, ingest_date: date) -> dict[str, int]:
    """Fetch every configured source. Overwrites the same ingest_date partition."""
    _prefer_ipv4()
    counts: dict[str, int] = {}
    with httpx.Client(
        base_url=settings.brasilapi_base_url.rstrip("/"),
        timeout=settings.http_timeout_seconds,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    ) as client:
        for source, spec in SOURCES.items():
            body, status, url = _get_with_retry(
                client, spec["path"], settings.http_max_retries
            )
            row_count = _land_bronze(
                data_dir=settings.data_dir,
                source=source,
                ingest_date=ingest_date,
                url=url,
                status_code=status,
                body=body,
            )
            counts[source] = row_count
            logger.info("bronze %s: %s rows -> %s", source, row_count, url)
    return counts


def _get_with_retry(
    client: httpx.Client, path: str, max_retries: int
) -> tuple[bytes, int, str]:
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(path)
            url = str(response.url)
            if response.status_code >= 500 or response.status_code == 429:
                raise ExtractError(f"{url} returned HTTP {response.status_code}")
            response.raise_for_status()
            return response.content, response.status_code, url
        except (httpx.HTTPError, ExtractError) as exc:
            last_exc = exc
            sleep_s = min(2 ** (attempt - 1), 8)
            logger.warning(
                "extract attempt %s/%s failed for %s: %s; retry in %ss",
                attempt,
                max_retries,
                path,
                exc,
                sleep_s,
            )
            time.sleep(sleep_s)
    raise ExtractError(f"failed to extract {path} after {max_retries} attempts: {last_exc}")


def _land_bronze(
    *,
    data_dir: Path,
    source: str,
    ingest_date: date,
    url: str,
    status_code: int,
    body: bytes,
) -> int:
    payload_path = bronze_payload_path(data_dir, source, ingest_date)
    manifest_path = bronze_manifest_path(data_dir, source, ingest_date)
    payload_path.parent.mkdir(parents=True, exist_ok=True)

    # Preserve the payload as landed. Re-runs for the same date overwrite in place.
    payload_path.write_bytes(body)

    parsed = json.loads(body)
    row_count = len(parsed) if isinstance(parsed, list) else 1
    manifest: dict[str, Any] = {
        "source": source,
        "url": url,
        "http_status": status_code,
        "ingest_date": ingest_date.isoformat(),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "row_count": row_count,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return row_count
