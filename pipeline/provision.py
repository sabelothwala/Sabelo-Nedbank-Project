"""Gold layer entry point.

Reads Silver Delta tables and produces the three Gold star-schema tables under
`/data/output/gold/`. Pandera enforces strict + ordered schemas at write time —
a column-order or field-count drift is caught here, not by the scoring harness.

Output paths and field counts are fixed by output_schema_spec.md:
  /data/output/gold/dim_customers/      9 fields  (customer_sk, customer_id,
                                                   gender, province, income_band,
                                                   segment, risk_score, kyc_status,
                                                   age_band)
  /data/output/gold/dim_accounts/      11 fields  (account_sk, account_id,
                                                   customer_id [pos 3, GAP-026],
                                                   account_type, account_status,
                                                   open_date, product_tier,
                                                   digital_channel, credit_limit,
                                                   current_balance,
                                                   last_activity_date)
  /data/output/gold/fact_transactions/ 15 fields  (transaction_sk, transaction_id,
                                                   account_sk, customer_sk,
                                                   transaction_date,
                                                   transaction_timestamp,
                                                   transaction_type,
                                                   merchant_category,
                                                   merchant_subcategory,
                                                   amount, currency, channel,
                                                   province, dq_flag,
                                                   ingestion_timestamp)

Surrogate keys
--------------
BIGINT, deterministic via `int_range(1, h+1)` after a stable sort on the
natural key. Same input data → same _sk values across re-runs.
No `monotonic_id` (non-deterministic across partitions); no hash-to-bigint
(avoids 64-bit collision risk).

age_band derivation
-------------------
floor((run_date - dob) / 365.25), bucketed via cfg.gold.age_band.buckets.
`run_date = cfg.run.run_date` (set by scorer) or today UTC. Building the
when/then/otherwise chain from the LOW bucket up wraps each new check around
the previous one — the last appended (highest min_age) becomes the outer
predicate, so the largest qualifying bucket wins. Ages <18 fall through to
NULL (spec: should not occur in source; do not silently drop).

dim_accounts.customer_id (GAP-026)
----------------------------------
Renamed from accounts.customer_ref. Without this field at position 3,
validation Q2 fails (zero tolerance).

dq_flag and merchant_subcategory
--------------------------------
Both carried from Silver as nullable Utf8 (Stage 1 → all-null). Trap E:
neither column is built fresh here, so we skip the explicit `pl.lit(None,
dtype=pl.Utf8)` cast — the Silver Delta read already returns proper Utf8.

ingestion_timestamp (Trap B — both sides)
-----------------------------------------
Carried from Silver `_ingested_at`. Read-side: the deltalake reader strips
the UTC tz label across the Bronze→Silver round-trip; we re-tag as UTC
(`replace_time_zone("UTC")`) so the in-memory column matches the Gold
schema's `Datetime("us", "UTC")` type for Pandera validation.
Write-side: `deltalake==0.15.3` (both pyarrow and rust engines) likewise
strips tz labels at write time, persisting `isAdjustedToUTC=false`. The
VALUES on disk are still UTC microseconds since epoch — only the label is
dropped. The Gold spec §2 lists ingestion_timestamp as TIMESTAMP (no tz
requirement); DuckDB and PySpark readers receive correct UTC values.
"""

from __future__ import annotations

import datetime as dt
import logging

import polars as pl

from pipeline.config import load_config
from pipeline.io_utils import advise_dontneed, write_delta
from pipeline.schemas import SCHEMA_REGISTRY

log = logging.getLogger("pipeline.provision")


# ── Helpers ─────────────────────────────────────────────────────────────────


def _read_silver(path: str) -> pl.DataFrame:
    """Read a Silver Delta and restore the UTC tz on `_ingested_at`."""
    return pl.read_delta(path).with_columns(
        pl.col("_ingested_at").dt.replace_time_zone("UTC")
    )


def _surrogate_key(natural_key: str, alias: str) -> tuple[pl.Expr, list[str]]:
    """Return (sort_keys, sk_alias). Caller does sort+with_row_index pattern."""
    return pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias(alias), [natural_key]


def _resolve_run_date(cfg: dict) -> dt.date:
    """Run date used for age derivation. cfg → today UTC fallback."""
    cfg_val = cfg["run"].get("run_date")
    if cfg_val is None:
        return dt.datetime.now(dt.timezone.utc).date()
    if isinstance(cfg_val, dt.date):
        return cfg_val
    if isinstance(cfg_val, str):
        return dt.date.fromisoformat(cfg_val)
    raise TypeError(f"run.run_date must be date or ISO string, got {type(cfg_val)}")


def _age_band_expr(buckets: list[dict], run_date: dt.date) -> pl.Expr:
    """Build the age_band when/then chain from the bucket config."""
    age = (
        ((pl.lit(run_date) - pl.col("dob")).dt.total_days() / 365.25)
        .floor()
        .cast(pl.Int32)
    )
    expr: pl.Expr = pl.lit(None, dtype=pl.Utf8)
    for bucket in sorted(buckets, key=lambda b: b["min_age"]):
        expr = pl.when(age >= bucket["min_age"]).then(pl.lit(bucket["label"])).otherwise(expr)
    return expr.alias("age_band")


# ── Per-table provisioners ──────────────────────────────────────────────────


def _provision_dim_customers(cfg: dict) -> int:
    silver_path = f"{cfg['output']['silver_path']}/customers"
    gold_path = f"{cfg['output']['gold_path']}/dim_customers"
    schema = SCHEMA_REGISTRY["DimCustomersSchema"]
    run_date = _resolve_run_date(cfg)
    buckets = cfg["gold"]["age_band"]["buckets"]

    df = (
        _read_silver(silver_path)
        .sort("customer_id")
        .with_columns(_age_band_expr(buckets, run_date))
        .with_columns(pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias("customer_sk"))
        .select([
            "customer_sk",
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "age_band",
        ])
    )

    null_age_bands = df.filter(pl.col("age_band").is_null()).height
    if null_age_bands > 0:
        log.warning(
            "dim_customers: %d rows with age<18 (age_band=NULL). "
            "Spec: do not silently drop; investigate source generator.",
            null_age_bands,
        )

    schema.validate(df)
    write_delta(df, gold_path, mode="overwrite")
    log.info("gold.dim_customers rows=%d run_date=%s path=%s", df.height, run_date, gold_path)
    return df.height


def _provision_dim_accounts(cfg: dict) -> int:
    silver_path = f"{cfg['output']['silver_path']}/accounts"
    gold_path = f"{cfg['output']['gold_path']}/dim_accounts"
    schema = SCHEMA_REGISTRY["DimAccountsSchema"]

    df = (
        _read_silver(silver_path)
        .rename({"customer_ref": "customer_id"})  # GAP-026
        .sort("account_id")
        .with_columns(pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias("account_sk"))
        .select([
            "account_sk",
            "account_id",
            "customer_id",  # POSITION 3 — GAP-026
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
        ])
    )

    schema.validate(df)
    write_delta(df, gold_path, mode="overwrite")
    log.info("gold.dim_accounts rows=%d path=%s", df.height, gold_path)
    return df.height


def _provision_fact_transactions(cfg: dict) -> int:
    """Build fact_transactions via a lazy Polars plan.

    Memory math for the eager version on a 1M-row Silver: the 22-col Silver
    frame (incl. audit hashes ~60 B/row) materialises at ~700 MB, then sort +
    join allocates ~equal-size temporaries, peaking past the 2 GB cap.

    Lazy plan with project-pushdown: only the 13 needed columns are scanned
    out of the Silver Delta (saves ~40% bytes/row), the sort runs on the slim
    frame, and the joins are projected likewise. Surrogate key is assigned
    AFTER sort but BEFORE the joins — joins may permute row order but each
    row carries its assigned `transaction_sk`, so the SK stays deterministic
    across re-runs (sort key = transaction_id).
    """
    silver_path = f"{cfg['output']['silver_path']}/transactions"
    gold_path = f"{cfg['output']['gold_path']}/fact_transactions"
    gold_root = cfg["output"]["gold_path"]
    schema = SCHEMA_REGISTRY["FactTransactionsSchema"]

    silver_height = pl.scan_delta(silver_path).select(pl.len()).collect().item()

    silver_txn = pl.scan_delta(silver_path).select([
        "transaction_id",
        "account_id",
        "transaction_date",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        "amount",
        "currency",
        "channel",
        "location_province",
        "dq_flag",
        "_ingested_at",
    ])
    dim_accounts_lazy = pl.scan_delta(f"{gold_root}/dim_accounts").select(
        ["account_id", "account_sk", "customer_id"]
    )
    dim_customers_lazy = pl.scan_delta(f"{gold_root}/dim_customers").select(
        ["customer_id", "customer_sk"]
    )

    df = (
        silver_txn
        .sort("transaction_id")
        .with_columns(pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias("transaction_sk"))
        .join(dim_accounts_lazy, on="account_id", how="inner")
        .join(dim_customers_lazy, on="customer_id", how="inner")
        .with_columns([
            pl.col("location_province").alias("province"),
            pl.col("_ingested_at").dt.replace_time_zone("UTC").alias("ingestion_timestamp"),
        ])
        .select([
            "transaction_sk",
            "transaction_id",
            "account_sk",
            "customer_sk",
            "transaction_date",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            "province",
            "dq_flag",
            "ingestion_timestamp",
        ])
        .collect()
    )

    if df.height != silver_height:
        raise ValueError(
            f"fact_transactions row count drift: silver={silver_height} "
            f"gold={df.height} — joins dropped rows. RI check in Silver should "
            "have caught this; investigate orphan customers/accounts."
        )

    schema.validate(df)
    write_delta(df, gold_path, mode="overwrite")
    log.info("gold.fact_transactions rows=%d path=%s", df.height, gold_path)
    return df.height


# ── Entry point ─────────────────────────────────────────────────────────────


def run_provisioning() -> None:
    cfg = load_config()
    log.info("provision start gold_path=%s", cfg["output"]["gold_path"])

    # Order matters: fact_transactions reads the dims, so dims must exist first.
    _provision_dim_customers(cfg)
    _provision_dim_accounts(cfg)
    _provision_fact_transactions(cfg)
    # Silver transactions parquet is the largest read in this stage; drop its
    # bind-mount file cache so it doesn't sit in cgroup-accounted memory while
    # the validate stage starts up. Trap H — Linux scoring host: quiet no-op.
    advise_dontneed(f"{cfg['output']['silver_path']}/transactions")

    log.info("provision done")
