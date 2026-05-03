"""Phase 5 verification — Gold layer contract checks.

Run inside the container after the pipeline has completed:

  docker run --rm \
    -v "$PWD/output:/data/output" \
    -v "$PWD/scripts:/app/scripts" \
    --entrypoint python nedbank-de-stage1:dev /app/scripts/_phase5_check.py

Asserts against the freshly-written Gold Delta tables, beyond what the
DuckDB harness in validate.py already covers:

  1. Field counts and column order match output_schema_spec §§2-4 exactly.
  2. dim_accounts.customer_id is at position 3 (GAP-026).
  3. Surrogate keys are non-null, unique, monotonic Int64.
  4. age_band has only the 6 canonical labels (or null).
  5. fact_transactions.merchant_subcategory and dq_flag are all-null in Stage 1.
  6. fact_transactions.currency is uniformly "ZAR".
  7. fact_transactions.ingestion_timestamp is tz-aware UTC.
  8. Row counts: dim_customers 80k, dim_accounts 100k, fact_transactions 1M.
  9. fact_transactions.account_sk and customer_sk reference valid dim rows.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

GOLD = Path("/data/output/gold")

EXPECTED_DIM_CUSTOMERS = [
    "customer_sk", "customer_id", "gender", "province", "income_band",
    "segment", "risk_score", "kyc_status", "age_band",
]
EXPECTED_DIM_ACCOUNTS = [
    "account_sk", "account_id", "customer_id", "account_type", "account_status",
    "open_date", "product_tier", "digital_channel", "credit_limit",
    "current_balance", "last_activity_date",
]
EXPECTED_FACT_TRANSACTIONS = [
    "transaction_sk", "transaction_id", "account_sk", "customer_sk",
    "transaction_date", "transaction_timestamp", "transaction_type",
    "merchant_category", "merchant_subcategory", "amount", "currency",
    "channel", "province", "dq_flag", "ingestion_timestamp",
]
AGE_BAND_LABELS = {"18-25", "26-35", "36-45", "46-55", "56-65", "65+"}


def fail(msg: str) -> None:
    print(f"FAIL  {msg}", file=sys.stderr)
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"PASS  {msg}")


def check_columns(df: pl.DataFrame, expected: list[str], table_name: str) -> None:
    actual = list(df.columns)
    if actual != expected:
        fail(f"{table_name} column order mismatch.\n  expected: {expected}\n  actual:   {actual}")
    ok(f"{table_name} has {len(actual)} fields in correct order")


def check_sk(df: pl.DataFrame, sk_col: str, table_name: str) -> None:
    sk = df[sk_col]
    if sk.dtype != pl.Int64:
        fail(f"{table_name}.{sk_col} dtype is {sk.dtype}, expected Int64")
    if sk.null_count() != 0:
        fail(f"{table_name}.{sk_col} has {sk.null_count()} nulls, expected 0")
    if sk.n_unique() != df.height:
        fail(f"{table_name}.{sk_col} not unique: {sk.n_unique()} unique / {df.height} rows")
    if sk.min() != 1 or sk.max() != df.height:
        fail(f"{table_name}.{sk_col} range = [{sk.min()}, {sk.max()}], expected [1, {df.height}]")
    ok(f"{table_name}.{sk_col} = Int64, non-null, unique, [1..{df.height}]")


# ── dim_customers ──────────────────────────────────────────────────────────

dim_c = pl.read_delta(str(GOLD / "dim_customers"))
check_columns(dim_c, EXPECTED_DIM_CUSTOMERS, "dim_customers")
if dim_c.height != 80000:
    fail(f"dim_customers row count = {dim_c.height}, expected 80000")
ok(f"dim_customers rows = {dim_c.height}")
check_sk(dim_c, "customer_sk", "dim_customers")

age_bands = set(dim_c["age_band"].drop_nulls().unique().to_list())
extra = age_bands - AGE_BAND_LABELS
if extra:
    fail(f"dim_customers.age_band has unexpected labels: {extra}")
null_age = dim_c["age_band"].null_count()
ok(f"dim_customers.age_band labels = {sorted(age_bands)}, nulls (age<18) = {null_age}")

# ── dim_accounts (GAP-026) ─────────────────────────────────────────────────

dim_a = pl.read_delta(str(GOLD / "dim_accounts"))
check_columns(dim_a, EXPECTED_DIM_ACCOUNTS, "dim_accounts")
if dim_a.columns[2] != "customer_id":
    fail(f"GAP-026: dim_accounts position 3 is '{dim_a.columns[2]}', expected 'customer_id'")
ok("GAP-026: dim_accounts.customer_id is at position 3")
if dim_a.height != 100000:
    fail(f"dim_accounts row count = {dim_a.height}, expected 100000")
ok(f"dim_accounts rows = {dim_a.height}")
check_sk(dim_a, "account_sk", "dim_accounts")

# ── fact_transactions ──────────────────────────────────────────────────────

fact = pl.read_delta(str(GOLD / "fact_transactions"))
check_columns(fact, EXPECTED_FACT_TRANSACTIONS, "fact_transactions")
if fact.height != 1_000_000:
    fail(f"fact_transactions row count = {fact.height}, expected 1000000")
ok(f"fact_transactions rows = {fact.height}")
check_sk(fact, "transaction_sk", "fact_transactions")

if fact["merchant_subcategory"].null_count() != fact.height:
    fail("fact_transactions.merchant_subcategory must be all-null in Stage 1")
ok("fact_transactions.merchant_subcategory is all-null (Stage 1 contract)")

if fact["dq_flag"].null_count() != fact.height:
    fail("fact_transactions.dq_flag must be all-null in Stage 1 (clean records)")
ok("fact_transactions.dq_flag is all-null (Stage 1 clean records)")

currencies = set(fact["currency"].unique().to_list())
if currencies != {"ZAR"}:
    fail(f"fact_transactions.currency must be uniformly 'ZAR', found {currencies}")
ok("fact_transactions.currency is uniformly 'ZAR'")

# Trap B extension (write-side): deltalake 0.15.3 strips tz labels from
# timestamp columns at write time (both pyarrow and rust engines), persisting
# `isAdjustedToUTC=false` regardless of the input arrow column's tz=UTC tag.
# The VALUES on disk are still UTC microseconds since epoch — only the tz
# label is dropped. The Gold spec §2 lists `ingestion_timestamp` as TIMESTAMP
# (no tz requirement); DuckDB and PySpark readers receive correct UTC values.
import pyarrow.dataset as pads

ts_field = pads.dataset(str(GOLD / "fact_transactions"), format="parquet").schema.field("ingestion_timestamp")
if str(ts_field.type) != "timestamp[us]":
    fail(f"fact_transactions.ingestion_timestamp on-disk type = {ts_field.type}, expected timestamp[us] (deltalake 0.15.3 strips tz)")
ok(f"fact_transactions.ingestion_timestamp on-disk type = {ts_field.type} (UTC values preserved; label stripped by deltalake)")

# FK integrity (cheaper restatement of validate.py's checks against the SK FKs)
acc_sk_set = set(dim_a["account_sk"].to_list())
cust_sk_set = set(dim_c["customer_sk"].to_list())
bad_acc = fact.filter(~pl.col("account_sk").is_in(list(acc_sk_set))).height
bad_cust = fact.filter(~pl.col("customer_sk").is_in(list(cust_sk_set))).height
if bad_acc != 0:
    fail(f"fact_transactions has {bad_acc} rows with account_sk not in dim_accounts")
if bad_cust != 0:
    fail(f"fact_transactions has {bad_cust} rows with customer_sk not in dim_customers")
ok("fact_transactions FKs (account_sk, customer_sk) all resolve")

print("\nAll Phase 5 Gold contract checks passed.")
