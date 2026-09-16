-- Recruiter sample queries. Run after `docker compose up` against the gold schema.
--   docker compose exec postgres psql -U pipeline -d br_open_data -f /dev/stdin < sql/examples/recruiter_queries.sql
-- Or: psql "$DSN" -f sql/examples/recruiter_queries.sql

-- 1. Latest official rates (the fact a DE would put on a dashboard).
SELECT
    f.snapshot_date,
    d.rate_name,
    f.rate_value,
    d.unit
FROM gold.fact_rate_snapshot AS f
JOIN gold.dim_rate AS d ON d.rate_code = f.rate_code
WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM gold.fact_rate_snapshot)
ORDER BY d.rate_code;

-- 2. Where Brazilian banks are headquartered (coverage aggregation).
SELECT
    state,
    bank_count,
    banks_with_compe,
    banks_with_headquarters
FROM gold.agg_bank_coverage_by_state
WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.agg_bank_coverage_by_state)
ORDER BY bank_count DESC
LIMIT 10;

-- 3. Institutions without a COMPE code (fintechs / settlement entities).
SELECT ispb, short_name, full_name, hq_city, hq_state
FROM gold.dim_bank
WHERE has_compe_code = FALSE
ORDER BY short_name
LIMIT 15;

-- 4. Prove idempotency: one row per rate per day, one run row per ingest date.
SELECT snapshot_date, COUNT(*) AS fact_rows
FROM gold.fact_rate_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;

SELECT * FROM gold.pipeline_run ORDER BY ingest_date DESC;
