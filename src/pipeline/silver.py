"""Silver: typed, deduped tables written as Parquet."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.paths import bronze_manifest_path, bronze_payload_path, silver_parquet_path
from pipeline.quality import assert_banks_silver, assert_taxas_silver
from pipeline.schema import RATE_METADATA

logger = logging.getLogger(__name__)


def transform_all(data_dir: Path, ingest_date: date) -> dict[str, int]:
    banks = transform_banks(data_dir, ingest_date)
    taxas = transform_taxas(data_dir, ingest_date)
    return {"banks": len(banks), "taxas": len(taxas)}


def transform_banks(data_dir: Path, ingest_date: date) -> pd.DataFrame:
    raw, extracted_at, endpoint = _read_bronze(data_dir, "banks", ingest_date)
    if not isinstance(raw, list):
        raise ValueError("banks bronze payload must be a JSON array")

    rows = []
    dropped_no_ispb = 0
    for item in raw:
        ispb = _normalize_ispb(item.get("ispb"))
        if ispb is None:
            dropped_no_ispb += 1
            continue
        addr = item.get("headquarters_address") or {}
        compe = item.get("code")
        rows.append(
            {
                "ispb": ispb,
                "compe_code": int(compe) if compe is not None else pd.NA,
                "short_name": _clean_str(item.get("name")),
                "full_name": _clean_str(item.get("fullName")) or _clean_str(item.get("name")),
                "cnpj": _digits_only(item.get("cnpj")),
                "hq_street": _clean_str(addr.get("street")),
                "hq_number": _clean_str(addr.get("number")),
                "hq_complement": _clean_str(addr.get("complement")),
                "hq_district": _clean_str(addr.get("district")),
                "hq_city": _clean_str(addr.get("city")),
                "hq_state": _clean_state(addr.get("state")),
                "hq_zip": _clean_str(addr.get("zipCode")),
                "address_source": _clean_str(item.get("address_source")),
                "address_confidence": _clean_str(item.get("address_confidence")),
                "has_compe_code": compe is not None,
                "has_headquarters": bool(addr),
                "ingest_date": ingest_date,
                "extracted_at": extracted_at,
                "source_endpoint": endpoint,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("banks silver is empty after dropping rows without ISPB")

    before = len(df)
    df = df.drop_duplicates(subset=["ispb"], keep="last")
    dupes = before - len(df)

    df["compe_code"] = df["compe_code"].astype("Int64")
    df["short_name"] = df["short_name"].astype("string")
    df["full_name"] = df["full_name"].astype("string")
    df["ispb"] = df["ispb"].astype("string")
    df["hq_state"] = df["hq_state"].astype("string")

    assert_banks_silver(df)
    _write_parquet(data_dir, "banks", ingest_date, df)
    logger.info(
        "silver banks: %s rows (dropped_no_ispb=%s, dropped_dup_ispb=%s)",
        len(df),
        dropped_no_ispb,
        dupes,
    )
    return df


def transform_taxas(data_dir: Path, ingest_date: date) -> pd.DataFrame:
    raw, extracted_at, endpoint = _read_bronze(data_dir, "taxas", ingest_date)
    if not isinstance(raw, list):
        raise ValueError("taxas bronze payload must be a JSON array")

    rows = []
    for item in raw:
        nome = _clean_str(item.get("nome"))
        valor = item.get("valor")
        if nome is None or valor is None:
            continue
        meta = RATE_METADATA.get(nome, {})
        rows.append(
            {
                "rate_code": meta.get("rate_code") or nome.upper(),
                "rate_name": nome,
                "rate_value": float(valor),
                "unit": meta.get("unit") or "percent",
                "ingest_date": ingest_date,
                "extracted_at": extracted_at,
                "source_endpoint": endpoint,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("taxas silver is empty")
    df = df.drop_duplicates(subset=["rate_code"], keep="last")
    df["rate_code"] = df["rate_code"].astype("string")
    df["rate_name"] = df["rate_name"].astype("string")

    assert_taxas_silver(df)
    _write_parquet(data_dir, "taxas", ingest_date, df)
    logger.info("silver taxas: %s rows (%s)", len(df), ", ".join(df["rate_code"]))
    return df


def _read_bronze(data_dir: Path, source: str, ingest_date: date) -> tuple[object, datetime, str]:
    payload_path = bronze_payload_path(data_dir, source, ingest_date)
    manifest_path = bronze_manifest_path(data_dir, source, ingest_date)
    if not payload_path.exists():
        raise FileNotFoundError(f"missing bronze payload: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    extracted_at = datetime.now(timezone.utc)
    endpoint = source
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        extracted_at = datetime.fromisoformat(manifest["extracted_at"])
        endpoint = manifest.get("url", source)
    return payload, extracted_at, endpoint


def _write_parquet(data_dir: Path, source: str, ingest_date: date, df: pd.DataFrame) -> None:
    path = silver_parquet_path(data_dir, source, ingest_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _normalize_ispb(value: object) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    return digits.zfill(8)


def _digits_only(value: object) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def _clean_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_state(value: object) -> str | None:
    text = _clean_str(value)
    if text is None:
        return None
    return text.upper()[:2]
