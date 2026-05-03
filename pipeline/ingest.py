"""Bronze layer entry point.

Three sources → three Delta tables under /data/output/bronze/. Bronze is raw:
CSVs are read with all source columns as Utf8 (`infer_schema_length=0`) so
nothing about the source bytes is interpreted at this layer. JSONL keeps its
JSON-native types (Float64 amount, Boolean retry_flag) — converting them to
strings here would *modify* the source, which Bronze is not allowed to do.

Audit columns are appended in a single `with_columns` call so the hash sees
source columns only — see io_utils.audit_expressions.

Transactions write path: chunked Python line iterator → pl.read_ndjson on each
chunk → unnest + audit → pyarrow ParquetWriter append → Delta from parquet.
Polars 0.20.31's streaming engine has no NDJSON source, so `scan_ndjson +
sink_parquet` errors with "not yet supported in standard engine". The chunked
iterator keeps peak memory bounded by `sources.transactions.streaming_chunk_rows`
(100k rows ≈ 80 MB) — strictly weaker than `.collect()` on the 1M-row frame,
which is the constraint we actually care about.

Why merchant_subcategory is added explicitly as `pl.lit(None, dtype=pl.Utf8)`:
the Stage 1 source omits the key entirely from every JSON object. Polars infers
`pl.Null` dtype for an all-missing column, which fails the Bronze schema
(expects Utf8). The cast in pl.lit makes the column Utf8 from the start. The
flattened struct fields get explicit casts for the same reason — a chunk where
every metadata.device_id is null infers `pl.Null` dtype, breaking the
ParquetWriter's cross-chunk schema requirement.
"""

from __future__ import annotations

import datetime as dt
import io
import logging
import os
from collections.abc import Iterator

import polars as pl
import pyarrow as pa
from deltalake import write_deltalake

from pipeline.config import load_config
from pipeline.io_utils import (
    audit_expressions,
    generate_batch_id,
    write_delta,
)
from pipeline.schemas import SCHEMA_REGISTRY

log = logging.getLogger("pipeline.ingest")

# Source column lists — used to compute _source_row_hash over source bytes only
# (excluding the audit columns themselves). These match the data dictionary.
_CUSTOMERS_SOURCE_COLS = (
    "customer_id", "id_number", "first_name", "last_name", "dob", "gender",
    "province", "income_band", "segment", "risk_score", "kyc_status",
    "product_flags",
)
_ACCOUNTS_SOURCE_COLS = (
    "account_id", "customer_ref", "account_type", "account_status",
    "open_date", "product_tier", "mobile_number", "digital_channel",
    "credit_limit", "current_balance", "last_activity_date",
)
_TRANSACTIONS_SOURCE_COLS = (
    "transaction_id", "account_id", "transaction_date", "transaction_time",
    "transaction_type", "merchant_category", "merchant_subcategory", "amount",
    "currency", "channel", "location_province", "location_city",
    "location_coordinates", "metadata_device_id", "metadata_session_id",
    "metadata_retry_flag",
)


def _ingest_customers(cfg: dict, batch_id: str, ingested_at: dt.datetime) -> int:
    src_cfg = cfg["sources"]["customers"]
    src_path = cfg["input"]["customers_path"]
    dst_path = f"{cfg['output']['bronze_path']}/customers"
    schema = SCHEMA_REGISTRY[src_cfg["schema_bronze"]]

    df = pl.read_csv(src_path, infer_schema_length=0).with_columns(
        audit_expressions(
            source_columns=_CUSTOMERS_SOURCE_COLS,
            source_system=src_cfg["source_system"],
            batch_id=batch_id,
            ingested_at=ingested_at,
        )
    )
    schema.validate(df)
    write_delta(df, dst_path, mode="overwrite")
    log.info("bronze.customers rows=%d path=%s", df.height, dst_path)
    return df.height


def _ingest_accounts(cfg: dict, batch_id: str, ingested_at: dt.datetime) -> int:
    src_cfg = cfg["sources"]["accounts"]
    src_path = cfg["input"]["accounts_path"]
    dst_path = f"{cfg['output']['bronze_path']}/accounts"
    schema = SCHEMA_REGISTRY[src_cfg["schema_bronze"]]

    df = pl.read_csv(src_path, infer_schema_length=0).with_columns(
        audit_expressions(
            source_columns=_ACCOUNTS_SOURCE_COLS,
            source_system=src_cfg["source_system"],
            batch_id=batch_id,
            ingested_at=ingested_at,
        )
    )
    schema.validate(df)
    write_delta(df, dst_path, mode="overwrite")
    log.info("bronze.accounts rows=%d path=%s", df.height, dst_path)
    return df.height


def _iter_jsonl_chunks(path: str, chunk_rows: int) -> Iterator[bytes]:
    """Yield raw JSONL bytes in chunks of `chunk_rows` lines.

    Reading line-by-line never holds more than `chunk_rows` source lines in
    memory; the OS streams the file. Each chunk is the concatenation of those
    lines, suitable for `pl.read_ndjson(BytesIO(...))`.

    On exit, advise the kernel to drop the page cache for this file. The
    transactions JSONL is ~600 MB and read exactly once per pipeline run;
    leaving it in the page cache eats cgroup-accounted memory (cgroup v2
    counts file cache from bind mounts) that the Silver `.collect()` later
    needs for its working set. Linux-only; harmless if unsupported.
    """
    fd = os.open(path, os.O_RDONLY)
    try:
        chunk: list[bytes] = []
        with os.fdopen(fd, "rb", closefd=False) as f:
            for line in f:
                chunk.append(line)
                if len(chunk) >= chunk_rows:
                    yield b"".join(chunk)
                    chunk = []
            if chunk:
                yield b"".join(chunk)
    finally:
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except (AttributeError, OSError):
            pass
        os.close(fd)


def _process_transactions_chunk(
    df: pl.DataFrame,
    *,
    source_system: str,
    batch_id: str,
    ingested_at: dt.datetime,
) -> pl.DataFrame:
    """Flatten nested objects, add merchant_subcategory + audit columns.

    Each struct field is cast to its expected Bronze dtype so a chunk where a
    nullable column happens to be all-null (Polars infers pl.Null) still
    produces a Utf8/Boolean column matching the schema. Without these casts,
    pyarrow ParquetWriter rejects later chunks for schema mismatch.
    """
    return (
        df.with_columns(
            [
                pl.col("location").struct.field("province").cast(pl.Utf8).alias("location_province"),
                pl.col("location").struct.field("city").cast(pl.Utf8).alias("location_city"),
                pl.col("location").struct.field("coordinates").cast(pl.Utf8).alias("location_coordinates"),
                pl.col("metadata").struct.field("device_id").cast(pl.Utf8).alias("metadata_device_id"),
                pl.col("metadata").struct.field("session_id").cast(pl.Utf8).alias("metadata_session_id"),
                pl.col("metadata").struct.field("retry_flag").cast(pl.Boolean).alias("metadata_retry_flag"),
            ]
        )
        .drop(["location", "metadata"])
        .with_columns(pl.lit(None, dtype=pl.Utf8).alias("merchant_subcategory"))
        .with_columns(
            audit_expressions(
                source_columns=_TRANSACTIONS_SOURCE_COLS,
                source_system=source_system,
                batch_id=batch_id,
                ingested_at=ingested_at,
            )
        )
    )


def _ingest_transactions(cfg: dict, batch_id: str, ingested_at: dt.datetime) -> int:
    """Stream JSONL chunks straight into the Bronze Delta table.

    Avoids the previous /tmp parquet round-trip — that intermediate file lived
    on the container's 512 MB tmpfs and counted against the 2 GB cgroup budget.
    Combined with delta-rs 0.15.3's eager pyarrow-engine materialisation when
    handed a `pyarrow.dataset`, the old path peaked at the full 2 GB cap during
    Bronze. Streaming RecordBatch-by-RecordBatch into `write_deltalake` keeps
    peak memory bounded by one chunk in flight (~80 MB per 100k rows).

    Schema is probed from the first chunk: we materialise it once for the
    Pandera check and to capture the arrow schema, then chain those batches
    with the remainder of the iterator into `write_deltalake`.
    """
    src_cfg = cfg["sources"]["transactions"]
    src_path = cfg["input"]["transactions_path"]
    dst_path = f"{cfg['output']['bronze_path']}/transactions"
    schema = SCHEMA_REGISTRY[src_cfg["schema_bronze"]]
    chunk_rows = src_cfg["streaming_chunk_rows"]

    chunk_iter = _iter_jsonl_chunks(src_path, chunk_rows)
    try:
        first_chunk_bytes = next(chunk_iter)
    except StopIteration:
        raise RuntimeError(f"transactions source is empty: {src_path}")

    first_df = _process_transactions_chunk(
        pl.read_ndjson(io.BytesIO(first_chunk_bytes)),
        source_system=src_cfg["source_system"],
        batch_id=batch_id,
        ingested_at=ingested_at,
    )
    # First chunk is a sufficient probe for column-level Pandera checks
    # (dtype, presence, strict, nullable-of-non-nullable). Uniqueness is
    # enforced at Silver, not Bronze.
    schema.validate(first_df)
    first_table = first_df.to_arrow()
    arrow_schema = first_table.schema
    first_batches = first_table.to_batches()
    rows = first_df.height
    del first_df, first_table

    def _all_batches() -> Iterator[pa.RecordBatch]:
        nonlocal rows
        for batch in first_batches:
            yield batch
        first_batches.clear()
        for chunk_bytes in chunk_iter:
            df_chunk = _process_transactions_chunk(
                pl.read_ndjson(io.BytesIO(chunk_bytes)),
                source_system=src_cfg["source_system"],
                batch_id=batch_id,
                ingested_at=ingested_at,
            )
            rows += df_chunk.height
            table = df_chunk.to_arrow()
            del df_chunk
            for batch in table.to_batches():
                yield batch
            del table

    write_deltalake(
        dst_path,
        data=_all_batches(),
        schema=arrow_schema,
        mode="overwrite",
        engine="rust",
        overwrite_schema=True,
    )

    log.info("bronze.transactions rows=%d path=%s", rows, dst_path)
    return rows


def run_ingestion() -> None:
    cfg = load_config()
    batch_id = cfg["run"].get("batch_id") or generate_batch_id()
    ingested_at = dt.datetime.now(dt.timezone.utc)
    log.info(
        "ingest start batch_id=%s ingested_at=%s",
        batch_id, ingested_at.isoformat(),
    )

    n_cust = _ingest_customers(cfg, batch_id, ingested_at)
    n_acc = _ingest_accounts(cfg, batch_id, ingested_at)
    n_txn = _ingest_transactions(cfg, batch_id, ingested_at)

    log.info(
        "ingest done customers=%d accounts=%d transactions=%d",
        n_cust, n_acc, n_txn,
    )
