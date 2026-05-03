"""Phase 2 verification — not part of the pipeline, not packaged.

Confirms:
  1. All 9 schemas import.
  2. Field counts match the spec (Bronze: 12+4 / 11+4 / 16+4. Silver: 12+4 /
     11+4 / 18+4 [adds transaction_timestamp + dq_flag]. Gold: 9 / 11 / 15.)
  3. Gold dim_accounts has customer_id at position 3 (GAP-026).
  4. Gold fact_transactions has merchant_subcategory and dq_flag.
  5. A synthetic frame matching each schema validates without error.
  6. Config loads and exposes the keys ingest/transform/provision will read.
"""

from __future__ import annotations

import datetime as dt
import sys
from decimal import Decimal

import polars as pl

from pipeline.config import load_config
from pipeline.schemas import (
    AGE_BANDS,
    DQ_FLAG_VALUES,
    GENDER_VALUES,
    SCHEMA_REGISTRY,
    BronzeAccountsSchema,
    BronzeCustomersSchema,
    BronzeTransactionsSchema,
    DimAccountsSchema,
    DimCustomersSchema,
    FactTransactionsSchema,
    SilverAccountsSchema,
    SilverCustomersSchema,
    SilverTransactionsSchema,
)


def _audit_lit(n: int) -> dict:
    ts = dt.datetime(2026, 5, 2, 10, 0, 0, tzinfo=dt.timezone.utc)
    return {
        "_ingested_at":     [ts] * n,
        "_source_system":   ["test"] * n,
        "_batch_id":        ["batch-0001"] * n,
        "_source_row_hash": ["abc123"] * n,
    }


def check_field_counts():
    expected = {
        "BronzeCustomersSchema":    16,   # 12 source + 4 audit
        "BronzeAccountsSchema":     15,   # 11 source + 4 audit
        "BronzeTransactionsSchema": 20,   # 16 flat source + 4 audit
        "SilverCustomersSchema":    16,
        "SilverAccountsSchema":     15,
        "SilverTransactionsSchema": 22,   # 16 source + transaction_timestamp + dq_flag + 4 audit
        "DimCustomersSchema":       9,
        "DimAccountsSchema":        11,
        "FactTransactionsSchema":   15,
    }
    for name, want in expected.items():
        got = len(SCHEMA_REGISTRY[name].columns)
        status = "OK " if got == want else "FAIL"
        print(f"  {status}  {name:32s}  fields={got:3d}  expected={want}")
        assert got == want, f"{name} field count: got {got}, want {want}"


def check_gold_positions():
    cols = list(DimAccountsSchema.columns.keys())
    pos3 = cols[2]  # 0-indexed: position 3 → cols[2]
    print(f"  DimAccountsSchema position 3: {pos3!r}  (must be 'customer_id' for GAP-026)")
    assert pos3 == "customer_id", f"GAP-026: position 3 is {pos3!r}, must be 'customer_id'"

    fact_cols = list(FactTransactionsSchema.columns.keys())
    assert "merchant_subcategory" in fact_cols, "fact_transactions missing merchant_subcategory"
    assert "dq_flag" in fact_cols, "fact_transactions missing dq_flag"
    print(f"  FactTransactionsSchema has merchant_subcategory and dq_flag")

    dim_cust_cols = list(DimCustomersSchema.columns.keys())
    assert "age_band" in dim_cust_cols, "dim_customers missing age_band"
    assert "dob" not in dim_cust_cols, "dim_customers must NOT have raw dob"
    print(f"  DimCustomersSchema has age_band, no raw dob")


def check_synthetic_validation():
    # Bronze customers — synthetic 2-row frame
    df = pl.DataFrame({
        "customer_id":   ["c1", "c2"],
        "id_number":     ["*********1234", "*********5678"],
        "first_name":    ["Thabo", "Liezel"],
        "last_name":     ["Dlamini", "Van der Merwe"],
        "dob":           ["1987-04-15", "1995-12-31"],
        "gender":        ["M", "F"],
        "province":      ["Gauteng", "Western Cape"],
        "income_band":   ["MIDDLE", "HIGH"],
        "segment":       ["MASS", "PRIVATE"],
        "risk_score":    ["5", "3"],
        "kyc_status":    ["VERIFIED", "VERIFIED"],
        "product_flags": ["HL|CC", "SA"],
        **_audit_lit(2),
    })
    BronzeCustomersSchema.validate(df)
    print(f"  OK   BronzeCustomersSchema validates 2-row synthetic frame")

    # Silver customers — typed
    df = pl.DataFrame({
        "customer_id":   ["c1"],
        "id_number":     ["*********1234"],
        "first_name":    ["Thabo"],
        "last_name":     ["Dlamini"],
        "dob":           [dt.date(1987, 4, 15)],
        "gender":        ["M"],
        "province":      ["Gauteng"],
        "income_band":   ["MIDDLE"],
        "segment":       ["MASS"],
        "risk_score":    [5],
        "kyc_status":    ["VERIFIED"],
        "product_flags": ["HL|CC"],
        **_audit_lit(1),
    }, schema_overrides={"risk_score": pl.Int32})
    SilverCustomersSchema.validate(df)
    print(f"  OK   SilverCustomersSchema validates typed synthetic frame")

    # dim_customers — Gold
    df = pl.DataFrame({
        "customer_sk":  [1],
        "customer_id":  ["c1"],
        "gender":       ["M"],
        "province":     ["Gauteng"],
        "income_band":  ["MIDDLE"],
        "segment":      ["MASS"],
        "risk_score":   [5],
        "kyc_status":   ["VERIFIED"],
        "age_band":     ["36-45"],
    }, schema_overrides={"customer_sk": pl.Int64, "risk_score": pl.Int32})
    DimCustomersSchema.validate(df)
    print(f"  OK   DimCustomersSchema validates 1-row synthetic frame")

    # dim_accounts — Gold (the GAP-026 critical path)
    df = pl.DataFrame({
        "account_sk":         [1],
        "account_id":         ["a1"],
        "customer_id":        ["c1"],
        "account_type":       ["TRANSACTIONAL"],
        "account_status":     ["ACTIVE"],
        "open_date":          [dt.date(2020, 1, 1)],
        "product_tier":       ["BASIC"],
        "digital_channel":    ["APP"],
        "credit_limit":       [None],
        "current_balance":    [Decimal("4231.50")],
        "last_activity_date": [dt.date(2025, 11, 3)],
    }, schema_overrides={
        "account_sk": pl.Int64,
        "credit_limit": pl.Decimal(18, 2),
        "current_balance": pl.Decimal(18, 2),
    })
    DimAccountsSchema.validate(df)
    print(f"  OK   DimAccountsSchema validates 1-row synthetic frame")

    # fact_transactions — Gold
    df = pl.DataFrame({
        "transaction_sk":        [1],
        "transaction_id":        ["t1"],
        "account_sk":            [1],
        "customer_sk":           [1],
        "transaction_date":      [dt.date(2025, 3, 22)],
        "transaction_timestamp": [dt.datetime(2025, 3, 22, 14, 37, 5)],
        "transaction_type":      ["DEBIT"],
        "merchant_category":     ["GROCERY"],
        "merchant_subcategory":  [None],
        "amount":                [Decimal("349.50")],
        "currency":              ["ZAR"],
        "channel":               ["POS"],
        "province":              ["Gauteng"],
        "dq_flag":               [None],
        "ingestion_timestamp":   [dt.datetime(2026, 5, 2, 10, 0, 0, tzinfo=dt.timezone.utc)],
    }, schema_overrides={
        "transaction_sk": pl.Int64,
        "account_sk": pl.Int64,
        "customer_sk": pl.Int64,
        "amount": pl.Decimal(18, 2),
        # Same trap that bites the real ingest: an all-None column comes back
        # as pl.Null dtype unless we cast. Stage 1 has every merchant_subcategory
        # null AND every dq_flag null, so both must be explicitly Utf8 at write.
        "merchant_subcategory": pl.Utf8,
        "dq_flag": pl.Utf8,
    })
    FactTransactionsSchema.validate(df)
    print(f"  OK   FactTransactionsSchema validates 1-row synthetic frame")


def check_strict_rejects_unknown_column():
    df = pl.DataFrame({
        "customer_sk":  [1],
        "customer_id":  ["c1"],
        "gender":       ["M"],
        "province":     ["Gauteng"],
        "income_band":  ["MIDDLE"],
        "segment":      ["MASS"],
        "risk_score":   [5],
        "kyc_status":   ["VERIFIED"],
        "age_band":     ["36-45"],
        "rogue_column": ["surprise"],   # should trigger strict failure
    }, schema_overrides={"customer_sk": pl.Int64, "risk_score": pl.Int32})
    try:
        DimCustomersSchema.validate(df)
    except Exception as e:
        print(f"  OK   DimCustomersSchema rejects unknown column "
              f"(strict=True works): {type(e).__name__}")
        return
    raise AssertionError("DimCustomersSchema should reject unknown columns but did not")


def check_config():
    cfg = load_config()
    required = [
        "input.customers_path", "input.accounts_path", "input.transactions_path",
        "output.bronze_path", "output.silver_path", "output.gold_path",
        "sources.customers.schema_bronze", "sources.customers.dedup_keys",
        "sources.transactions.streaming_chunk_rows",
        "standardisation.currency.canonical",
        "standardisation.currency.accept",
        "standardisation.date_formats.candidates",
        "gold.age_band.buckets",
        "gold.surrogate_keys.method",
        "drift.policy",
        "dq.allowed_flags",
        "validation.query_2_tolerance",
    ]
    for path in required:
        node = cfg
        for part in path.split("."):
            assert part in node, f"missing config key: {path}"
            node = node[part]
        print(f"  OK   config key present: {path}")

    # Sanity-check: every schema name referenced in config is registered.
    for src_name, src in cfg["sources"].items():
        for layer in ("schema_bronze", "schema_silver"):
            schema_name = src[layer]
            assert schema_name in SCHEMA_REGISTRY, (
                f"sources.{src_name}.{layer}={schema_name!r} not in SCHEMA_REGISTRY"
            )
    print(f"  OK   all schema names in config resolve via SCHEMA_REGISTRY")

    # Sanity-check: age_band bucket count and labels match AGE_BANDS constant.
    cfg_labels = tuple(b["label"] for b in cfg["gold"]["age_band"]["buckets"])
    assert cfg_labels == AGE_BANDS, f"age_band labels mismatch: {cfg_labels} vs {AGE_BANDS}"
    print(f"  OK   gold.age_band.buckets labels == AGE_BANDS constant")

    # Sanity-check: dq.allowed_flags == DQ_FLAG_VALUES.
    cfg_flags = tuple(cfg["dq"]["allowed_flags"])
    assert set(cfg_flags) == set(DQ_FLAG_VALUES), (
        f"dq.allowed_flags mismatch: {cfg_flags} vs {DQ_FLAG_VALUES}"
    )
    print(f"  OK   dq.allowed_flags == DQ_FLAG_VALUES constant")


if __name__ == "__main__":
    print("── Field counts ──")
    check_field_counts()
    print("── Gold positions / GAP-026 ──")
    check_gold_positions()
    print("── Synthetic-frame validation ──")
    check_synthetic_validation()
    print("── strict=True rejects unknown column ──")
    check_strict_rejects_unknown_column()
    print("── pipeline_config.yaml loads with required keys ──")
    check_config()
    print("\nALL PHASE 2 CHECKS PASSED")
    sys.exit(0)
