"""Data-quality gates. Hard failures raise AssertionError and stop the job."""

from __future__ import annotations

import logging

import pandas as pd

from pipeline.schema import DQ

logger = logging.getLogger(__name__)


def assert_banks_silver(df: pd.DataFrame) -> None:
    _min_rows(df, DQ["banks_min_rows"], "banks")
    _unique_not_null(df, "ispb", "banks")
    invalid_ispb = ~df["ispb"].astype("string").str.fullmatch(r"\d{8}", na=False)
    if invalid_ispb.any():
        raise AssertionError(
            f"banks: {int(invalid_ispb.sum())} rows have ISPB that is not 8 digits"
        )
    _null_rate(df, "short_name", 0.0, "banks")
    _null_rate(df, "full_name", 0.0, "banks")
    logger.info("dq banks silver ok: rows=%s", len(df))


def assert_taxas_silver(df: pd.DataFrame) -> None:
    _min_rows(df, DQ["taxas_min_rows"], "taxas")
    _unique_not_null(df, "rate_code", "taxas")
    _null_rate(df, "rate_value", 0.0, "taxas")
    if (df["rate_value"] <= 0).any():
        raise AssertionError("taxas: rate_value must be > 0")
    if (df["rate_value"] >= DQ["taxas_max_value"]).any():
        raise AssertionError(
            f"taxas: rate_value must be < {DQ['taxas_max_value']} (percent scale)"
        )
    logger.info("dq taxas silver ok: rows=%s", len(df))


def _min_rows(df: pd.DataFrame, minimum: int, label: str) -> None:
    if len(df) < minimum:
        raise AssertionError(f"{label}: expected >= {minimum} rows, got {len(df)}")


def _unique_not_null(df: pd.DataFrame, column: str, label: str) -> None:
    if df[column].isna().any():
        raise AssertionError(f"{label}: {column} contains nulls")
    if not df[column].is_unique:
        dupes = int(df[column].duplicated().sum())
        raise AssertionError(f"{label}: {column} is not unique ({dupes} duplicates)")


def _null_rate(df: pd.DataFrame, column: str, max_rate: float, label: str) -> None:
    rate = float(df[column].isna().mean()) if len(df) else 1.0
    if rate > max_rate:
        raise AssertionError(
            f"{label}: {column} null rate {rate:.2%} exceeds max {max_rate:.2%}"
        )
