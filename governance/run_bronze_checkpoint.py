"""Validate the bronze layer with Great Expectations.

Runs the `bronze_transactions` suite against a bronze NDJSON file and
exits non-zero if any expectation fails, so it can gate a pipeline step
(Dagster asset, CI job) rather than being a manual notebook exercise.

    python3 governance/run_bronze_checkpoint.py bronze_sample/bronze.ndjson

These are the *aggregate* checks -- uniqueness across the whole batch,
value distributions, non-empty batches. Per-record structural validation
happens earlier, at ingestion, in common/validation.py.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations import expectations as gxe

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "dining", "fuel",
    "entertainment", "utilities", "healthcare", "clothing", "online_retail",
]

FRAUD_PATTERNS = ["none", "high_amount", "odd_hour", "impossible_travel"]

SUITE_NAME = "bronze_transactions"


def build_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=SUITE_NAME)
    for expectation in [
        # Identity
        gxe.ExpectColumnValuesToNotBeNull(column="transaction_id"),
        gxe.ExpectColumnValuesToBeUnique(column="transaction_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="account_id"),
        # Amounts: positive, and bounded well above anything the generator
        # emits (its high_amount fraud pattern caps at $9k) so a hit here
        # means a data bug rather than a fraud signal.
        gxe.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0, max_value=50000, strict_min=True
        ),
        # Categorical domains
        gxe.ExpectColumnValuesToBeInSet(
            column="merchant_category", value_set=MERCHANT_CATEGORIES
        ),
        gxe.ExpectColumnValuesToBeInSet(column="is_fraud", value_set=[0, 1]),
        gxe.ExpectColumnValuesToBeInSet(
            column="fraud_pattern", value_set=FRAUD_PATTERNS
        ),
        # Geo bounds, constrained to the continental US rather than the
        # global [-90,90]/[-180,180] ranges. Global bounds do NOT reliably
        # catch a lat/lon swap: New York's (40.7, -74.0) swaps to a
        # latitude of -74.0, which is still globally valid. Tightening to
        # the actual operating geography makes any swap fail.
        gxe.ExpectColumnValuesToBeBetween(column="lat", min_value=24, max_value=50),
        gxe.ExpectColumnValuesToBeBetween(column="lon", min_value=-125, max_value=-66),
        # Enrichment applied at ingestion must be present.
        gxe.ExpectColumnValuesToNotBeNull(column="ingested_at"),
        gxe.ExpectColumnValuesToNotBeNull(column="dt"),
        # Catches a silently-empty ingestion run, not just bad rows.
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        # Fraud prevalence should stay in a plausible band. Far outside it
        # means the generator or the ingestion filter has drifted.
        gxe.ExpectColumnMeanToBeBetween(column="is_fraud", min_value=0.0, max_value=0.25),
    ]:
        suite.add_expectation(expectation)
    return suite


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?", default="bronze_sample/bronze.ndjson")
    args = parser.parse_args()

    source = Path(args.path)
    if not source.exists():
        sys.exit(
            f"no bronze file at {source}. Generate one first:\n"
            "  python3 data_generator/make_bronze_sample.py --single-file"
        )

    df = pd.read_json(source, lines=True)
    print(f"loaded {len(df):,} records from {source}")

    context = gx.get_context(mode="ephemeral")
    batch_definition = (
        context.data_sources.add_pandas("bronze_source")
        .add_dataframe_asset("transactions")
        .add_batch_definition_whole_dataframe("all_records")
    )

    suite = context.suites.add(build_suite())
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="bronze_validation", data=batch_definition, suite=suite
        )
    )

    result = validation_definition.run(batch_parameters={"dataframe": df})

    passed = sum(1 for r in result.results if r.success)
    total = len(result.results)
    print(f"\n{passed}/{total} expectations passed")

    for r in result.results:
        if not r.success:
            cfg = r.expectation_config
            column = cfg.kwargs.get("column", "-")
            print(f"  FAILED {cfg.type} on {column}: {r.result}")

    if not result.success:
        sys.exit(1)
    print("bronze layer PASSED all expectations")


if __name__ == "__main__":
    main()
