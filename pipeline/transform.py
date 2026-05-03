"""Silver layer entry point.

Reads each Bronze Delta, applies typing + dedup + standardisation, and writes
the typed Silver Delta. Pandera enforces the Silver contract at write time.

Operations per source
---------------------
customers     dob → Date, risk_score → Int32, dedup on customer_id.
accounts      open_date / last_activity_date → Date,
              credit_limit / current_balance → Decimal(18,2),
              empty CSV strings normalised to nulls before the cast,
              dedup on account_id.
transactions  transaction_date → Date,
              transaction_timestamp = transaction_date + transaction_time → Datetime("us"),
              amount → Decimal(18,2),
              currency normalised through `standardisation.currency.accept`,
              dq_flag added as nullable Utf8 (all-null in Stage 1),
              dedup on transaction_id, processed in chunks so the 1M-row
              frame is only fully resident at the dedup/validate step.

Multi-format date parsing is wired through but Stage 1 has only `%Y-%m-%d` in
config; Stage 2 enables `%d/%m/%Y` and `epoch_s` by adding entries to
`standardisation.date_formats.candidates`.

Currency normalisation applies strip+upper to the input and looks up the
result in `standardisation.currency.accept`. Stage 1 source is uniform "ZAR";
the lookup runs anyway so Stage 2's `R`/`rands`/`710`/`zar` variants drop in
without code change.

Schema drift policy (`drift.policy: quarantine`)
------------------------------------------------
Stage 1 enforces the policy via Pandera `strict=True`:
  Tier 1 (known cols): typed and written normally.
  Tier 2 (unknown cols): would be quarantined to /data/output/silver/_quarantine/
    — but Bronze write is also strict, so unknown source columns are caught at
    Bronze, not Silver. The quarantine writer is a Stage 2 build-out.
  Tier 3 (missing required cols): hard fail via Pandera. Pipeline exits non-zero.

Referential integrity stats are logged after all three Silver writes:
  accounts.customer_ref → customers.customer_id  (must be 0 orphans Stage 1)
  transactions.account_id → accounts.account_id  (must be 0 orphans Stage 1)
Stage 2 will populate dq_flag = ORPHANED_ACCOUNT for transaction orphans
rather than logging.

Why we materialise transactions for dedup
-----------------------------------------
`unique(subset=…)` over 1M rows with `transaction_id` uniqueness needs a
global view. Per-chunk dedup misses dupes that span chunks. The streaming
write into /tmp parquet keeps peak memory bounded during the type-cast leg;
the materialise-and-dedup step at the end peaks at ~600 MB for the 1M-row
Silver frame, which fits the 2 GB cap. Stage 2 (3M rows ≈ 1.8 GB) will need
an out-of-core dedup — DuckDB on the parquet temp would be the obvious move.
"""

from __future__ import annotations

import logging

import polars as pl

from pipeline._memlog import release_heap
from pipeline.config import load_config
from pipeline.io_utils import advise_dontneed, write_delta
from pipeline.schemas import SCHEMA_REGISTRY

log = logging.getLogger("pipeline.transform")


# ── Reusable expressions ────────────────────────────────────────────────────


def _restore_audit_tz() -> pl.Expr:
    """Re-tag `_ingested_at` as UTC after a Bronze Delta read.

    The deltalake Rust writer stores timestamps as parquet INT64 microseconds
    with `isAdjustedToUTC=true`, but pyarrow's reader maps this to tz-naive
    Datetime. The values are correct (UTC microseconds since epoch) — only
    the tz label is missing. `replace_time_zone("UTC")` restores the label
    without converting values.
    """
    return pl.col("_ingested_at").dt.replace_time_zone("UTC")


def _read_bronze(path: str) -> pl.DataFrame:
    return pl.read_delta(path).with_columns(_restore_audit_tz())


def _empty_to_null(col_name: str) -> pl.Expr:
    """Replace empty Utf8 with null. CSV's '' is not the same as missing —
    Bronze stores raw bytes; Silver wants real nulls before type casting.
    """
    c = pl.col(col_name)
    return pl.when(c == "").then(None).otherwise(c).alias(col_name)


def _parse_date(col_name: str, formats: list[str]) -> pl.Expr:
    """Coalesce-based multi-format date parser.

    For each candidate in `formats`, attempt parsing with `strict=False`
    (failures → null) and fill_null forward. The first format that successfully
    parses a row wins. Stage 1 has 1 candidate; Stage 2 adds DD/MM/YYYY and
    epoch_s by appending to the config list.
    """
    c = pl.col(col_name)
    parsed: pl.Expr | None = None
    for fmt in formats:
        if fmt == "epoch_s":
            this = pl.from_epoch(
                c.cast(pl.Int64, strict=False), time_unit="s"
            ).cast(pl.Date, strict=False)
        else:
            this = c.str.to_date(fmt, strict=False)
        parsed = this if parsed is None else parsed.fill_null(this)
    return parsed.alias(col_name)


def _normalize_currency_expr(currency_map: dict, canonical: str) -> pl.Expr:
    """Map raw currency to canonical via the strip+upper-normalised lookup.

    `default=canonical` coerces unknown variants to ZAR. Stage 2 adds dq_flag
    population for those rows; Stage 1 leaves dq_flag null since the source
    has no variants.
    """
    normalized = {k.upper().strip(): v for k, v in currency_map.items()}
    return (
        pl.col("currency")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_uppercase()
        .replace(normalized, default=canonical)
        .alias("currency")
    )


def _dedup(df: pl.DataFrame, subset: list[str]) -> pl.DataFrame:
    """Sort by (_ingested_at, _source_row_hash) then keep first per `subset`.

    The hash secondary key gives a deterministic winner when two duplicates
    share an ingestion timestamp (Stage 2 fintech double-delivery scenario).
    Stage 1 has no dupes — this is a no-op.
    """
    return (
        df.sort(["_ingested_at", "_source_row_hash"])
        .unique(subset=subset, keep="first", maintain_order=True)
    )


# ── Per-source transforms ───────────────────────────────────────────────────


def _transform_customers(cfg: dict) -> tuple[int, int]:
    src_cfg = cfg["sources"]["customers"]
    bronze_path = f"{cfg['output']['bronze_path']}/customers"
    silver_path = f"{cfg['output']['silver_path']}/customers"
    schema = SCHEMA_REGISTRY[src_cfg["schema_silver"]]
    formats = cfg["standardisation"]["date_formats"]["candidates"]

    df = _read_bronze(bronze_path)
    rows_in = df.height
    df = df.with_columns(
        [
            _parse_date("dob", formats),
            pl.col("risk_score").cast(pl.Int32),
        ]
    )
    df = _dedup(df, src_cfg["dedup_keys"])
    rows_out = df.height

    schema.validate(df)
    write_delta(df, silver_path, mode="overwrite")
    log.info(
        "silver.customers rows_in=%d rows_out=%d dupes_removed=%d",
        rows_in, rows_out, rows_in - rows_out,
    )
    return rows_in, rows_out


def _transform_accounts(cfg: dict) -> tuple[int, int]:
    src_cfg = cfg["sources"]["accounts"]
    bronze_path = f"{cfg['output']['bronze_path']}/accounts"
    silver_path = f"{cfg['output']['silver_path']}/accounts"
    schema = SCHEMA_REGISTRY[src_cfg["schema_silver"]]
    formats = cfg["standardisation"]["date_formats"]["candidates"]

    df = _read_bronze(bronze_path)
    rows_in = df.height
    df = (
        df.with_columns(
            [
                _empty_to_null("mobile_number"),
                _empty_to_null("credit_limit"),
                _empty_to_null("last_activity_date"),
            ]
        )
        .with_columns(
            [
                _parse_date("open_date", formats),
                _parse_date("last_activity_date", formats),
                pl.col("credit_limit").cast(pl.Decimal(18, 2)),
                pl.col("current_balance").cast(pl.Decimal(18, 2)),
            ]
        )
    )
    df = _dedup(df, src_cfg["dedup_keys"])
    rows_out = df.height

    schema.validate(df)
    write_delta(df, silver_path, mode="overwrite")
    log.info(
        "silver.accounts rows_in=%d rows_out=%d dupes_removed=%d",
        rows_in, rows_out, rows_in - rows_out,
    )
    return rows_in, rows_out


def _transform_transactions(cfg: dict) -> tuple[int, int]:
    """Build Silver transactions via a single lazy plan over the Bronze Delta.

    The earlier path wrote a /tmp parquet from chunked Polars frames, then
    re-read it for global dedup. Two costs piled up:
      • The /tmp parquet (~700 MB at 1 M rows) lived on tmpfs, which counts
        against the cgroup memory budget.
      • `pl.read_parquet` materialised an in-memory copy alongside the tmpfs
        file, peaking past the 2 GB cap before the file was unlinked.

    This version skips the temp parquet entirely. `pl.scan_delta` over the
    Bronze Delta files lets Polars apply projection pushdown and run the sort
    + unique + cast pipeline as a single optimised plan. Peak in-memory
    footprint is one materialised 1 M-row frame instead of "tmpfs file plus
    in-memory frame".

    Trap C does not apply here. The decimal cast is inside the lazy plan, but
    there is no parquet round-trip between the cast and validate, so the
    precision metadata stays intact.
    """
    src_cfg = cfg["sources"]["transactions"]
    bronze_path = f"{cfg['output']['bronze_path']}/transactions"
    silver_path = f"{cfg['output']['silver_path']}/transactions"
    schema = SCHEMA_REGISTRY[src_cfg["schema_silver"]]
    formats = cfg["standardisation"]["date_formats"]["candidates"]
    currency_map = cfg["standardisation"]["currency"]["accept"]
    canonical = cfg["standardisation"]["currency"]["canonical"]

    rows_in = pl.scan_delta(bronze_path).select(pl.len()).collect().item()

    df = (
        pl.scan_delta(bronze_path)
        .with_columns(_restore_audit_tz())
        .with_columns(
            # transaction_timestamp first — needs raw Utf8 transaction_date.
            pl.concat_str(
                [pl.col("transaction_date"), pl.lit(" "), pl.col("transaction_time")]
            )
            .str.to_datetime("%Y-%m-%d %H:%M:%S", time_unit="us", strict=False)
            .alias("transaction_timestamp"),
        )
        .with_columns(
            [
                _parse_date("transaction_date", formats),
                _normalize_currency_expr(currency_map, canonical),
                pl.lit(None, dtype=pl.Utf8).alias("dq_flag"),
            ]
        )
        .sort(["_ingested_at", "_source_row_hash"])
        .unique(subset=src_cfg["dedup_keys"], keep="first", maintain_order=True)
        .with_columns(pl.col("amount").cast(pl.Decimal(18, 2)))
        .collect()
    )
    rows_out = df.height

    schema.validate(df)
    write_delta(df, silver_path, mode="overwrite")

    log.info(
        "silver.transactions rows_in=%d rows_out=%d dupes_removed=%d",
        rows_in, rows_out, rows_in - rows_out,
    )
    return rows_in, rows_out


# ── Referential integrity ───────────────────────────────────────────────────


def _check_referential_integrity(cfg: dict) -> dict[str, int]:
    """Log orphan counts for the two FK relationships. Stage 1 must be 0/0."""
    silver = cfg["output"]["silver_path"]

    customers = pl.read_delta(f"{silver}/customers").select("customer_id")
    accounts = pl.read_delta(f"{silver}/accounts").select(["account_id", "customer_ref"])
    orphan_acc = (
        accounts.join(customers, left_on="customer_ref", right_on="customer_id", how="anti")
        .height
    )

    txn_account_ids = pl.read_delta(f"{silver}/transactions").select("account_id")
    orphan_txn = (
        txn_account_ids.join(accounts.select("account_id"), on="account_id", how="anti")
        .height
    )

    log.info(
        "ri.accounts→customers orphans=%d / %d (%.4f%%)",
        orphan_acc, accounts.height, 100.0 * orphan_acc / max(accounts.height, 1),
    )
    log.info(
        "ri.transactions→accounts orphans=%d / %d (%.4f%%)",
        orphan_txn, txn_account_ids.height,
        100.0 * orphan_txn / max(txn_account_ids.height, 1),
    )

    if orphan_acc > 0 or orphan_txn > 0:
        log.warning(
            "Stage 1 expected 0 orphans on clean data; got accounts=%d transactions=%d. "
            "Stage 2 will populate dq_flag=ORPHANED_ACCOUNT instead of logging.",
            orphan_acc, orphan_txn,
        )

    return {"accounts": orphan_acc, "transactions": orphan_txn}


# ── Entry point ─────────────────────────────────────────────────────────────


def run_transformation() -> None:
    cfg = load_config()
    log.info("transform start drift_policy=%s", cfg["drift"]["policy"])

    _transform_customers(cfg)
    _transform_accounts(cfg)
    release_heap()
    _transform_transactions(cfg)
    release_heap()
    _check_referential_integrity(cfg)
    # Bronze is fully consumed — no downstream stage needs it. On
    # Docker-for-Mac the bind-mount page cache for Bronze parquet files
    # stays charged to the cgroup until evicted (Trap H), and that page
    # cache stacks on top of Silver/Gold reads in `provision`, eating
    # ~80 MiB of headroom on the fact_transactions write peak. Drop it
    # now. Linux scoring host: quiet no-op.
    advise_dontneed(cfg["output"]["bronze_path"])

    log.info("transform done")
