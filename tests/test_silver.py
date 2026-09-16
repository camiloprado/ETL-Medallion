from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from pipeline.quality import assert_banks_silver, assert_taxas_silver
from pipeline.schema import DQ
from pipeline.silver import transform_banks, transform_taxas

FIXTURES = Path(__file__).parent / "fixtures" / "sample_payloads.json"
INGEST = date(2026, 9, 16)


def _land(tmp_path: Path, source: str, payload: object) -> None:
    part = tmp_path / "bronze" / source / f"ingest_date={INGEST.isoformat()}"
    part.mkdir(parents=True)
    (part / "payload.json").write_text(json.dumps(payload), encoding="utf-8")
    (part / "_manifest.json").write_text(
        json.dumps(
            {
                "source": source,
                "url": f"https://brasilapi.com.br/api/{source}/v1",
                "http_status": 200,
                "ingest_date": INGEST.isoformat(),
                "extracted_at": datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc).isoformat(),
                "row_count": len(payload) if isinstance(payload, list) else 1,
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture
def relax_bank_min(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(DQ, "banks_min_rows", 1)


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    raw = json.loads(FIXTURES.read_text(encoding="utf-8"))
    _land(tmp_path, "banks", raw["banks"])
    _land(tmp_path, "taxas", raw["taxas"])
    return tmp_path


def test_banks_silver_dedupes_and_drops_missing_ispb(lake: Path, relax_bank_min: None) -> None:
    df = transform_banks(lake, INGEST)
    assert set(df["ispb"]) == {"00000000", "60701190", "00038121"}
    assert df["ispb"].is_unique
    bb = df.loc[df["ispb"] == "00000000"].iloc[0]
    assert bb["hq_state"] == "DF"
    assert bool(bb["has_compe_code"]) is True
    selic = df.loc[df["ispb"] == "00038121"].iloc[0]
    assert bool(selic["has_compe_code"]) is False
    assert bool(selic["has_headquarters"]) is False
    assert (lake / "silver" / "banks" / "ingest_date=2026-09-16" / "banks.parquet").exists()


def test_taxas_silver_maps_codes(lake: Path) -> None:
    df = transform_taxas(lake, INGEST)
    assert list(df["rate_code"]) == ["SELIC", "CDI", "IPCA"]
    assert df.loc[df["rate_code"] == "SELIC", "rate_value"].iloc[0] == 14.0
    assert df.loc[df["rate_code"] == "IPCA", "unit"].iloc[0] == "percent_12_months"


def test_dq_fails_on_too_few_banks() -> None:
    df = pd.DataFrame(
        {
            "ispb": ["00000000"],
            "short_name": ["BB"],
            "full_name": ["Banco do Brasil"],
        }
    )
    with pytest.raises(AssertionError, match="expected >= 100"):
        assert_banks_silver(df)


def test_dq_fails_on_duplicate_ispb(relax_bank_min: None) -> None:
    df = pd.DataFrame(
        {
            "ispb": ["00000000", "00000000"],
            "short_name": ["A", "B"],
            "full_name": ["A", "B"],
        }
    )
    with pytest.raises(AssertionError, match="not unique"):
        assert_banks_silver(df)


def test_dq_fails_on_non_positive_rate() -> None:
    df = pd.DataFrame(
        {
            "rate_code": ["SELIC"],
            "rate_value": [0.0],
        }
    )
    with pytest.raises(AssertionError, match="> 0"):
        assert_taxas_silver(df)
