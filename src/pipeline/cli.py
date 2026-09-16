"""CLI: python -m pipeline run"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from pipeline.config import load_settings
from pipeline.extract import extract_all
from pipeline.gold import load_gold
from pipeline.silver import transform_all

LAYERS = ("bronze", "silver", "gold")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description="BrasilAPI medallion pipeline: bronze JSON → silver Parquet → gold Postgres.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Run extract, transform, and load (default path).")
    run.add_argument(
        "--layers",
        default="bronze,silver,gold",
        help="Comma-separated layers to run: bronze,silver,gold",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    if args.command == "run":
        return _run(args.layers)
    return 1


def _run(layers_csv: str) -> int:
    layers = tuple(part.strip() for part in layers_csv.split(",") if part.strip())
    unknown = [layer for layer in layers if layer not in LAYERS]
    if unknown:
        print(f"unknown layers: {unknown}. Use {LAYERS}", file=sys.stderr)
        return 2

    settings = load_settings()
    ingest_date: date = settings.partition_date()
    logging.getLogger(__name__).info(
        "run start ingest_date=%s data_dir=%s layers=%s",
        ingest_date,
        settings.data_dir.resolve(),
        ",".join(layers),
    )

    bronze_counts: dict[str, int] = {}
    silver_counts: dict[str, int] = {}
    gold_counts: dict[str, int] = {}

    if "bronze" in layers:
        bronze_counts = extract_all(settings, ingest_date)
    if "silver" in layers:
        silver_counts = transform_all(settings.data_dir, ingest_date)
    if "gold" in layers:
        gold_counts = load_gold(settings, ingest_date)

    _print_summary(ingest_date, bronze_counts, silver_counts, gold_counts)
    return 0


def _print_summary(
    ingest_date: date,
    bronze: dict[str, int],
    silver: dict[str, int],
    gold: dict[str, int],
) -> None:
    print()
    print(f"Medallion run complete  ingest_date={ingest_date.isoformat()}")
    if bronze:
        print(f"  bronze  banks={bronze.get('banks', 0)}  taxas={bronze.get('taxas', 0)}")
    if silver:
        print(f"  silver  banks={silver.get('banks', 0)}  taxas={silver.get('taxas', 0)}")
    if gold:
        print(
            "  gold    "
            f"dim_bank={gold.get('dim_bank', 0)}  "
            f"facts={gold.get('fact_rate_snapshot', 0)}  "
            f"coverage_states={gold.get('agg_bank_coverage_by_state', 0)}"
        )
    print("  Re-run is idempotent: same ingest_date overwrites bronze/silver and upserts gold.")
