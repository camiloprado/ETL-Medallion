-- Gold warehouse schema. Applied automatically by Postgres on first boot
-- (docker-entrypoint-initdb.d) and again by the pipeline (CREATE IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS gold;

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
);

CREATE TABLE IF NOT EXISTS gold.dim_rate (
    rate_code VARCHAR(16) PRIMARY KEY,
    rate_name TEXT NOT NULL,
    description TEXT NOT NULL,
    unit TEXT NOT NULL
);

INSERT INTO gold.dim_rate (rate_code, rate_name, description, unit) VALUES
    ('SELIC', 'Selic', 'Selic overnight policy rate (BCB).', 'percent_per_year'),
    ('CDI', 'CDI', 'Certificado de Depósito Interbancário — interbank deposit rate.', 'percent_per_year'),
    ('IPCA', 'IPCA', 'Índice Nacional de Preços ao Consumidor Amplo — 12-month inflation.', 'percent_12_months')
ON CONFLICT (rate_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS gold.fact_rate_snapshot (
    snapshot_date DATE NOT NULL,
    rate_code VARCHAR(16) NOT NULL REFERENCES gold.dim_rate (rate_code),
    rate_value NUMERIC(10, 4) NOT NULL,
    source_extracted_at TIMESTAMPTZ NOT NULL,
    ingest_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, rate_code)
);

CREATE TABLE IF NOT EXISTS gold.agg_bank_coverage_by_state (
    as_of_date DATE NOT NULL,
    state VARCHAR(2) NOT NULL,
    bank_count INTEGER NOT NULL,
    banks_with_compe INTEGER NOT NULL,
    banks_with_headquarters INTEGER NOT NULL,
    PRIMARY KEY (as_of_date, state)
);

CREATE TABLE IF NOT EXISTS gold.pipeline_run (
    ingest_date DATE PRIMARY KEY,
    banks_bronze_rows INTEGER NOT NULL,
    banks_silver_rows INTEGER NOT NULL,
    taxas_bronze_rows INTEGER NOT NULL,
    taxas_silver_rows INTEGER NOT NULL,
    fact_rows_upserted INTEGER NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON SCHEMA gold IS 'Analytics tables produced by the BrasilAPI medallion pipeline.';
COMMENT ON TABLE gold.dim_bank IS 'Brazilian financial institutions keyed by ISPB. Upserted each run; first/last_seen_date track catalog membership.';
COMMENT ON TABLE gold.dim_rate IS 'Official Brazilian rates published by BrasilAPI (Selic, CDI, IPCA).';
COMMENT ON TABLE gold.fact_rate_snapshot IS 'Daily snapshot of official rates. Grain: (snapshot_date, rate_code). Idempotent upsert.';
COMMENT ON TABLE gold.agg_bank_coverage_by_state IS 'Bank counts by HQ UF as of each ingest date. Rebuilt for that date on every run.';
COMMENT ON TABLE gold.pipeline_run IS 'One row per ingest_date with layer counts. Re-runs update the same date.';
