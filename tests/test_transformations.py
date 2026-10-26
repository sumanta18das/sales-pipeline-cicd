"""
Unit tests for src/transformations.py

Run locally:   pytest -v
Run in CI:     automatically on every Pull Request (.github/workflows/ci.yml)
"""

import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from transformations import (
    RAW_COLUMNS,
    add_ingestion_metadata,
    build_daily_sales,
    clean_orders,
    run_quality_checks,
    table_name,
)

SAMPLE_FILE = Path(__file__).resolve().parents[1] / "data" / "orders.csv"


def make_raw(spark, rows):
    """Build a bronze-like DataFrame (all columns as text) from a list of tuples."""
    return spark.createDataFrame(rows, RAW_COLUMNS)


# ---------------------------------------------------------------------------
# table_name
# ---------------------------------------------------------------------------
def test_table_name_builds_three_level_name():
    assert table_name("dev_catalog", "sales", "orders") == "dev_catalog.sales.orders"


@pytest.mark.parametrize("bad", ["", "dev-catalog", "dev catalog", "x;DROP TABLE y"])
def test_table_name_rejects_unsafe_names(bad):
    with pytest.raises(ValueError):
        table_name(bad, "sales", "orders")


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
def test_add_ingestion_metadata_adds_audit_columns(spark):
    raw = make_raw(
        spark, [("O1", "C1", "2026-10-01", "Mouse", "Electronics", "1", "10.00", "COMPLETED")]
    )
    out = add_ingestion_metadata(raw, "/tmp/orders.csv")

    assert out.columns == RAW_COLUMNS + ["_ingested_at", "_source_file"]
    assert out.first()["_source_file"] == "/tmp/orders.csv"


def test_add_ingestion_metadata_fails_when_column_missing(spark):
    raw = spark.createDataFrame([("O1",)], ["order_id"])
    with pytest.raises(ValueError, match="missing columns"):
        add_ingestion_metadata(raw, "x.csv")


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
def test_clean_orders_keeps_good_row_and_computes_amount(spark):
    raw = make_raw(
        spark, [("O1", "C1", "2026-10-01", "Mouse", "electronics", "2", "799.50", " completed ")]
    )
    row = clean_orders(raw).first()

    assert row["order_date"] == datetime.date(2026, 10, 1)
    assert row["quantity"] == 2
    assert row["amount"] == Decimal("1599.00")
    assert row["status"] == "COMPLETED"
    assert row["category"] == "Electronics"


def test_clean_orders_removes_duplicates(spark):
    row = ("O1", "C1", "2026-10-01", "Mouse", "Electronics", "1", "10.00", "COMPLETED")
    out = clean_orders(make_raw(spark, [row, row]))
    assert out.count() == 1


@pytest.mark.parametrize(
    "bad_row, reason",
    [
        (("O1", "", "2026-10-01", "P", "Home", "1", "10", "COMPLETED"), "blank customer"),
        (("", "C1", "2026-10-01", "P", "Home", "1", "10", "COMPLETED"), "blank order id"),
        (("O1", "C1", "2026-13-45", "P", "Home", "1", "10", "COMPLETED"), "invalid date"),
        (("O1", "C1", "2026-10-01", "P", "Home", "-2", "10", "COMPLETED"), "negative quantity"),
        (("O1", "C1", "2026-10-01", "P", "Home", "0", "10", "COMPLETED"), "zero quantity"),
        (("O1", "C1", "2026-10-01", "P", "Home", "1", "abc", "COMPLETED"), "text price"),
    ],
)
def test_clean_orders_drops_bad_rows(spark, bad_row, reason):
    out = clean_orders(make_raw(spark, [bad_row]))
    assert out.count() == 0, f"Row with {reason} should be dropped"


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------
def test_build_daily_sales_aggregates_and_skips_cancelled(spark):
    raw = make_raw(
        spark,
        [
            ("O1", "C1", "2026-10-01", "Mouse", "Electronics", "2", "100.00", "COMPLETED"),
            ("O2", "C2", "2026-10-01", "Cable", "Electronics", "1", "50.00", "SHIPPED"),
            ("O3", "C3", "2026-10-01", "Speaker", "Electronics", "1", "999.00", "CANCELLED"),
            ("O4", "C4", "2026-10-02", "Shirt", "Clothing", "3", "10.00", "COMPLETED"),
        ],
    )
    gold = {
        (r["order_date"].isoformat(), r["category"]): r
        for r in build_daily_sales(clean_orders(raw)).collect()
    }

    assert len(gold) == 2
    elec = gold[("2026-10-01", "Electronics")]
    assert elec["total_orders"] == 2
    assert elec["total_quantity"] == 3
    assert elec["total_revenue"] == Decimal("250.00")
    assert gold[("2026-10-02", "Clothing")]["total_revenue"] == Decimal("30.00")


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------
def test_quality_checks_pass_on_clean_data(spark):
    raw = make_raw(
        spark, [("O1", "C1", "2026-10-01", "Mouse", "Electronics", "2", "100.00", "COMPLETED")]
    )
    silver = clean_orders(raw)
    assert run_quality_checks(silver, build_daily_sales(silver)) == []


def test_quality_checks_catch_empty_tables(spark):
    empty = clean_orders(make_raw(spark, [("O1", "", "x", "P", "H", "1", "1", "COMPLETED")]))
    errors = run_quality_checks(empty, build_daily_sales(empty))
    assert any("silver_orders: table is empty" in e for e in errors)
    assert any("gold_daily_sales: table is empty" in e for e in errors)


# ---------------------------------------------------------------------------
# End to end on the real sample file (what the job will do in Databricks)
# ---------------------------------------------------------------------------
def test_full_pipeline_on_sample_file(spark):
    pdf = pd.read_csv(SAMPLE_FILE, dtype=str, keep_default_na=False)
    bronze = add_ingestion_metadata(spark.createDataFrame(pdf), str(SAMPLE_FILE))
    silver = clean_orders(bronze)
    gold = build_daily_sales(silver)

    assert bronze.count() == 31  # raw file rows (with 1 duplicate and bad rows)
    assert silver.count() == 26  # 1 duplicate + 4 bad rows removed
    assert run_quality_checks(silver, gold) == []
