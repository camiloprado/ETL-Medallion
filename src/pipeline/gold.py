"""Gold: load silver Parquet into Postgres dimensions, facts, and aggregations."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

from pipeline.config import Settings
from pipeline.paths import bronze_manifest_path, silver_parquet_path
from pipeline.schema import RATE_METADATA

logger = logging.getLogger(__name__)


def _executemany(conn: psycopg.Connection, query: str, rows: list) -> None:
    with conn.cursor() as cur:
        cur.executemany(query, rows)


def load_gold(settings: Settings, ingest_date: date) -> dict[str, int]:
    banks = pd.read_parquet(silver_parquet_path(settings.data_dir, "banks", ingest_date))
    taxas = pd.read_parquet(silver_parquet_path(settings.data_dir, "taxas", ingest_date))

    with psycopg.connect(settings.dsn(), autocommit=False) as conn:
        _ensure_schema(conn)
        bank_n = _upsert_dim_bank(conn, banks, ingest_date)
        rate_n = _upsert_dim_rate(conn, taxas)
        fact_n = _upsert_fact_rate_snapshot(conn, taxas, ingest_date)
        agg_n = _replace_bank_coverage(conn, banks, ingest_date)
        _upsert_pipeline_run(
            conn,
            ingest_date=ingest_date,
            banks_bronze=_bronze_rows(settings.data_dir, "banks", ingest_date),
            banks_silver=len(banks),
            taxas_bronze=_bronze_rows(settings.data_dir, "taxas", ingest_date),
            taxas_silver=len(taxas),
            fact_rows=fact_n,
        )
        _assert_gold(conn, ingest_date, banks, taxas)
        conn.commit()

    logger.info(
        "gold loaded: dim_bank=%s dim_rate=%s facts=%s coverage_rows=%s",
        bank_n,
        rate_n,
        fact_n,
        agg_n,
    )
    return {
        "dim_bank": bank_n,
        "dim_rate": rate_n,
        "fact_rate_snapshot": fact_n,
        "agg_bank_coverage_by_state": agg_n,
    }


def _ensure_schema(conn: psycopg.Connection) -> None:
    """Idempotent DDL so the job works even if init scripts did not run."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.dim_bank (
            ispb CHAR(8) PRIMARY KEY,
            compe_code INTEGER,
            short_name TEXT NOT NULL,
            full_name TEXT NOT NULL,
            cnpj VARCHAR(18),
            hq_city TEXT,
            hq_state VARCHAR(2),
            hq_zip VARCHAR(10),
            has_compe_code BOOLEAN NOT NULL,
            has_headquarters BOOLEAN NOT NULL,
            address_confidence TEXT,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.dim_rate (
            rate_code VARCHAR(16) PRIMARY KEY,
            rate_name TEXT NOT NULL,
            description TEXT NOT NULL,
            unit TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.fact_rate_snapshot (
            snapshot_date DATE NOT NULL,
            rate_code VARCHAR(16) NOT NULL REFERENCES gold.dim_rate (rate_code),
            rate_value NUMERIC(10, 4) NOT NULL,
            source_extracted_at TIMESTAMPTZ NOT NULL,
            ingest_date DATE NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (snapshot_date, rate_code)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.agg_bank_coverage_by_state (
            as_of_date DATE NOT NULL,
            state VARCHAR(2) NOT NULL,
            bank_count INTEGER NOT NULL,
            banks_with_compe INTEGER NOT NULL,
            banks_with_headquarters INTEGER NOT NULL,
            PRIMARY KEY (as_of_date, state)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.pipeline_run (
            ingest_date DATE PRIMARY KEY,
            banks_bronze_rows INTEGER NOT NULL,
            banks_silver_rows INTEGER NOT NULL,
            taxas_bronze_rows INTEGER NOT NULL,
            taxas_silver_rows INTEGER NOT NULL,
            fact_rows_upserted INTEGER NOT NULL,
            finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _upsert_dim_bank(conn: psycopg.Connection, banks: pd.DataFrame, ingest_date: date) -> int:
    rows = []
    for rec in banks.to_dict(orient="records"):
        rows.append(
            (
                rec["ispb"],
                _none_if_na(rec.get("compe_code")),
                rec["short_name"],
                rec["full_name"],
                _none_if_na(rec.get("cnpj")),
                _none_if_na(rec.get("hq_city")),
                _none_if_na(rec.get("hq_state")),
                _none_if_na(rec.get("hq_zip")),
                bool(rec["has_compe_code"]),
                bool(rec["has_headquarters"]),
                _none_if_na(rec.get("address_confidence")),
                ingest_date,
                ingest_date,
            )
        )
    _executemany(conn,
        """
        INSERT INTO gold.dim_bank (
            ispb, compe_code, short_name, full_name, cnpj,
            hq_city, hq_state, hq_zip, has_compe_code, has_headquarters,
            address_confidence, first_seen_date, last_seen_date, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (ispb) DO UPDATE SET
            compe_code = EXCLUDED.compe_code,
            short_name = EXCLUDED.short_name,
            full_name = EXCLUDED.full_name,
            cnpj = EXCLUDED.cnpj,
            hq_city = EXCLUDED.hq_city,
            hq_state = EXCLUDED.hq_state,
            hq_zip = EXCLUDED.hq_zip,
            has_compe_code = EXCLUDED.has_compe_code,
            has_headquarters = EXCLUDED.has_headquarters,
            address_confidence = EXCLUDED.address_confidence,
            first_seen_date = LEAST(gold.dim_bank.first_seen_date, EXCLUDED.first_seen_date),
            last_seen_date = GREATEST(gold.dim_bank.last_seen_date, EXCLUDED.last_seen_date),
            updated_at = NOW()
        """,
        rows,
    )
    return len(rows)


def _upsert_dim_rate(conn: psycopg.Connection, taxas: pd.DataFrame) -> int:
    name_by_code = {meta["rate_code"]: nome for nome, meta in RATE_METADATA.items()}
    desc_by_code = {meta["rate_code"]: meta["description"] for meta in RATE_METADATA.values()}
    rows = []
    for rec in taxas.to_dict(orient="records"):
        code = rec["rate_code"]
        rows.append(
            (
                code,
                rec["rate_name"] or name_by_code.get(code, code),
                desc_by_code.get(code, f"Official rate published by BrasilAPI as {rec['rate_name']}."),
                rec["unit"],
            )
        )
    _executemany(conn,
        """
        INSERT INTO gold.dim_rate (rate_code, rate_name, description, unit)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (rate_code) DO UPDATE SET
            rate_name = EXCLUDED.rate_name,
            description = EXCLUDED.description,
            unit = EXCLUDED.unit
        """,
        rows,
    )
    return len(rows)


def _upsert_fact_rate_snapshot(
    conn: psycopg.Connection, taxas: pd.DataFrame, ingest_date: date
) -> int:
    rows = []
    for rec in taxas.to_dict(orient="records"):
        extracted_at = rec["extracted_at"]
        if isinstance(extracted_at, str):
            extracted_at = datetime.fromisoformat(extracted_at)
        if getattr(extracted_at, "tzinfo", None) is None:
            extracted_at = extracted_at.replace(tzinfo=timezone.utc)
        rows.append(
            (
                ingest_date,
                rec["rate_code"],
                float(rec["rate_value"]),
                extracted_at,
                ingest_date,
            )
        )
    _executemany(conn,
        """
        INSERT INTO gold.fact_rate_snapshot (
            snapshot_date, rate_code, rate_value, source_extracted_at, ingest_date, loaded_at
        ) VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (snapshot_date, rate_code) DO UPDATE SET
            rate_value = EXCLUDED.rate_value,
            source_extracted_at = EXCLUDED.source_extracted_at,
            ingest_date = EXCLUDED.ingest_date,
            loaded_at = NOW()
        """,
        rows,
    )
    return len(rows)


def _replace_bank_coverage(conn: psycopg.Connection, banks: pd.DataFrame, ingest_date: date) -> int:
    work = banks.copy()
    work["state"] = work["hq_state"].fillna("??").astype(str).str.slice(0, 2)
    grouped = (
        work.groupby("state", dropna=False)
        .agg(
            bank_count=("ispb", "size"),
            banks_with_compe=("has_compe_code", "sum"),
            banks_with_headquarters=("has_headquarters", "sum"),
        )
        .reset_index()
    )
    conn.execute(
        "DELETE FROM gold.agg_bank_coverage_by_state WHERE as_of_date = %s",
        (ingest_date,),
    )
    rows = [
        (
            ingest_date,
            rec["state"],
            int(rec["bank_count"]),
            int(rec["banks_with_compe"]),
            int(rec["banks_with_headquarters"]),
        )
        for rec in grouped.to_dict(orient="records")
    ]
    _executemany(conn,
        """
        INSERT INTO gold.agg_bank_coverage_by_state (
            as_of_date, state, bank_count, banks_with_compe, banks_with_headquarters
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )
    return len(rows)


def _upsert_pipeline_run(
    conn: psycopg.Connection,
    *,
    ingest_date: date,
    banks_bronze: int,
    banks_silver: int,
    taxas_bronze: int,
    taxas_silver: int,
    fact_rows: int,
) -> None:
    conn.execute(
        """
        INSERT INTO gold.pipeline_run (
            ingest_date, banks_bronze_rows, banks_silver_rows,
            taxas_bronze_rows, taxas_silver_rows, fact_rows_upserted, finished_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (ingest_date) DO UPDATE SET
            banks_bronze_rows = EXCLUDED.banks_bronze_rows,
            banks_silver_rows = EXCLUDED.banks_silver_rows,
            taxas_bronze_rows = EXCLUDED.taxas_bronze_rows,
            taxas_silver_rows = EXCLUDED.taxas_silver_rows,
            fact_rows_upserted = EXCLUDED.fact_rows_upserted,
            finished_at = NOW()
        """,
        (ingest_date, banks_bronze, banks_silver, taxas_bronze, taxas_silver, fact_rows),
    )


def _assert_gold(
    conn: psycopg.Connection,
    ingest_date: date,
    banks: pd.DataFrame,
    taxas: pd.DataFrame,
) -> None:
    fact_count = conn.execute(
        """
        SELECT COUNT(*) FROM gold.fact_rate_snapshot
        WHERE snapshot_date = %s
        """,
        (ingest_date,),
    ).fetchone()[0]
    if fact_count != len(taxas):
        raise AssertionError(
            f"gold facts for {ingest_date}: expected {len(taxas)} rows, got {fact_count}"
        )

    dupes = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT snapshot_date, rate_code, COUNT(*) AS n
            FROM gold.fact_rate_snapshot
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        ) d
        """
    ).fetchone()[0]
    if dupes:
        raise AssertionError(f"gold fact_rate_snapshot has {dupes} duplicate keys")

    seen_today = conn.execute(
        "SELECT COUNT(*) FROM gold.dim_bank WHERE last_seen_date = %s",
        (ingest_date,),
    ).fetchone()[0]
    if seen_today != len(banks):
        raise AssertionError(
            f"gold dim_bank last_seen_date={ingest_date}: expected {len(banks)}, got {seen_today}"
        )

    coverage_sum = conn.execute(
        "SELECT COALESCE(SUM(bank_count), 0) FROM gold.agg_bank_coverage_by_state WHERE as_of_date = %s",
        (ingest_date,),
    ).fetchone()[0]
    if int(coverage_sum) != len(banks):
        raise AssertionError(
            f"gold coverage sum {coverage_sum} != silver banks {len(banks)}"
        )
    logger.info("dq gold ok")


def _bronze_rows(data_dir: Path, source: str, ingest_date: date) -> int:
    path = bronze_manifest_path(data_dir, source, ingest_date)
    if not path.exists():
        return 0
    import json

    return int(json.loads(path.read_text(encoding="utf-8")).get("row_count", 0))


def _none_if_na(value: object) -> object:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        return value
    return value
