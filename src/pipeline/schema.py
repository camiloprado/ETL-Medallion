"""Documented silver/gold schemas. Recruiters can read this file without running the job."""

from __future__ import annotations

# BrasilAPI sources this job extracts. Both are public, stable, and need no API key.
SOURCES = {
    "banks": {
        "path": "/api/banks/v1",
        "why": (
            "Full BCB COMPE participant catalog (~470 institutions). Natural "
            "slowly-changing dimension: ISPB, COMPE code, legal name, HQ address."
        ),
    },
    "taxas": {
        "path": "/api/taxas/v1",
        "why": (
            "Current Selic, CDI, and IPCA. Tiny daily snapshot that becomes a "
            "time-series fact when partitioned by ingest date."
        ),
    },
}

# Silver: banks — one row per ISPB after cleaning.
BANKS_SILVER_COLUMNS = {
    "ispb": "string, 8-digit BCB identifier, natural key",
    "compe_code": "Int64, COMPE/STR code (nullable — not every participant has one)",
    "short_name": "string, abbreviated legal name",
    "full_name": "string, full legal name",
    "cnpj": "string, tax id when BrasilAPI provides it (often null today)",
    "hq_street": "string, headquarters street",
    "hq_number": "string, headquarters number",
    "hq_complement": "string, complement",
    "hq_district": "string, neighborhood",
    "hq_city": "string, city",
    "hq_state": "string, UF (2 letters)",
    "hq_zip": "string, CEP",
    "address_source": "string, how HQ was resolved",
    "address_confidence": "string, BrasilAPI confidence label",
    "has_compe_code": "bool",
    "has_headquarters": "bool",
    "ingest_date": "date, lake partition",
    "extracted_at": "timestamp (UTC)",
    "source_endpoint": "string",
}

# Silver: taxas — one row per official rate on the ingest date.
TAXAS_SILVER_COLUMNS = {
    "rate_code": "string, SELIC | CDI | IPCA (stable surrogate key)",
    "rate_name": "string, original BrasilAPI nome",
    "rate_value": "float64, percent as published",
    "unit": "string, percent_per_year or percent_12_months",
    "ingest_date": "date, lake partition / snapshot date",
    "extracted_at": "timestamp (UTC)",
    "source_endpoint": "string",
}

RATE_METADATA = {
    "Selic": {
        "rate_code": "SELIC",
        "unit": "percent_per_year",
        "description": "Selic overnight policy rate (BCB).",
    },
    "CDI": {
        "rate_code": "CDI",
        "unit": "percent_per_year",
        "description": "Certificado de Depósito Interbancário — interbank deposit rate.",
    },
    "IPCA": {
        "rate_code": "IPCA",
        "unit": "percent_12_months",
        "description": "Índice Nacional de Preços ao Consumidor Amplo — 12-month inflation.",
    },
}

# Hard data-quality thresholds. Fail the job if these are missed.
DQ = {
    "banks_min_rows": 100,
    "taxas_min_rows": 1,
    "taxas_max_value": 100.0,  # percent; guards against unit mistakes
}
