"""I/O utilities for Delta reads/writes and audit metadata.

Used by every stage. The deltalake package (Rust engine) is the writer — no
JVM, no Spark session, no shuffle scheduler. Polars is the in-process engine.

Audit metadata, added at Bronze and preserved through Silver, is the POPIA §19
processing trail: every row carries (when, from-where, in-which-batch, hash-of-
source-bytes) so a downstream consumer can reproduce a row's lineage without
re-reading the source.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import secrets
from collections.abc import Iterator
from pathlib import Path
from typing import Iterable

import polars as pl
import pyarrow as pa
from deltalake import write_deltalake

log = logging.getLogger("pipeline.io")

_BATCH_ID_FORMAT = "%Y%m%dT%H%M%SZ"


def generate_batch_id(now: dt.datetime | None = None) -> str:
    """Per-run id: yyyyMMddTHHMMSSZ-<6-hex>.

    Timestamp prefix sorts runs lexicographically; hex suffix avoids collisions
    when two runs land in the same wall-clock second.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    return f"{now.strftime(_BATCH_ID_FORMAT)}-{secrets.token_hex(3)}"


def audit_expressions(
    *,
    source_columns: Iterable[str],
    source_system: str,
    batch_id: str,
    ingested_at: dt.datetime,
) -> list[pl.Expr]:
    """Polars expressions that materialise the four audit columns.

    Used inside a single `.with_columns(...)` call so every expression sees
    the pre-audit frame — `_source_row_hash` therefore hashes the source
    columns only, never the audit metadata itself.

    `_source_row_hash` is xxhash64 (UInt64), stringified. Cryptographic
    strength is unnecessary for drift detection; pure-Python sha256 over the
    1M-row transactions frame costs ~30 s of wall clock. Collision probability
    on 1M rows is ~5e-8 — irrelevant at our scale.
    """
    return [
        pl.lit(ingested_at).cast(pl.Datetime("us", "UTC")).alias("_ingested_at"),
        pl.lit(source_system, dtype=pl.Utf8).alias("_source_system"),
        pl.lit(batch_id, dtype=pl.Utf8).alias("_batch_id"),
        pl.struct([pl.col(c) for c in source_columns])
        .hash(seed=0)
        .cast(pl.Utf8)
        .alias("_source_row_hash"),
    ]


_WRITE_SLICE_ROWS = 100_000


def _cast_batch_to_schema(
    batch: pa.RecordBatch, target_schema: pa.Schema
) -> pa.RecordBatch:
    """Rebuild `batch` so its schema matches `target_schema`.

    `pa.RecordBatch` has no `cast()` method (only `pa.Table` does), and
    going Batch→Table→cast→Batches per write slice would re-allocate the
    full table — defeating the streaming-write savings. So cast at the
    Array level: each column is individually cast (a no-copy metadata
    change for tz-strip on timestamp arrays) and a new RecordBatch is
    constructed from the casted columns.
    """
    casted = []
    for i, target_field in enumerate(target_schema):
        col = batch.column(i)
        if col.type != target_field.type:
            col = col.cast(target_field.type)
        casted.append(col)
    return pa.RecordBatch.from_arrays(casted, schema=target_schema)


def _strip_timestamp_tz(schema: pa.Schema) -> pa.Schema:
    """Return `schema` with any tz-aware timestamp[us, tz=…] field replaced by
    its tz-naive `timestamp[us]` counterpart.

    Trap B says deltalake 0.15.3 strips tz labels on write regardless of arrow
    input — but only on the single-Table code path. The streaming
    `Iterator[RecordBatch]` code path preserves the tz, which makes the
    on-disk parquet `timestamp[us, tz=UTC]` instead of `timestamp[us]`.
    `_phase5_check.py` and `output_schema_spec.md §2` both pin the contract
    to `timestamp[us]` (UTC values, no label). Reproduce the old behaviour
    explicitly so the streaming write produces an identical on-disk schema.

    Casting an array from tz-aware to tz-naive is a metadata-only operation
    in arrow — no value change — because the underlying int64 microseconds
    are already epoch-UTC.
    """
    needs_change = any(
        pa.types.is_timestamp(f.type) and f.type.tz is not None for f in schema
    )
    if not needs_change:
        return schema
    fields = []
    for f in schema:
        if pa.types.is_timestamp(f.type) and f.type.tz is not None:
            fields.append(pa.field(f.name, pa.timestamp(f.type.unit), nullable=f.nullable))
        else:
            fields.append(f)
    return pa.schema(fields)


def write_delta(
    frame: pl.DataFrame,
    path: str | Path,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Write a materialised polars DataFrame to a Delta table in row-group
    slices, never holding the full polars frame and a full arrow Table at
    the same time.

    The earlier implementation called `frame.to_arrow()` once: a 1 M-row
    Silver write held the original polars frame (~600 MiB) AND a freshly-
    allocated arrow Table (~600 MiB) simultaneously while delta-rs ran the
    write — peak around 1.2 GiB just for the materialised representations.
    On the 2 GiB cgroup cap with bind-mount page cache stacked on top, that
    is the spike that drove the cold peak past 1.7 GiB.

    The streaming form yields one slice's RecordBatches into
    `write_deltalake`'s rust engine. Polars `iter_slices` returns zero-copy
    views into the underlying frame; only the per-slice arrow conversion
    (~50 MiB for 100 k rows × 22 cols) is fresh allocation, and the slice
    Table goes out of scope as soon as the writer pulls its batches. Peak
    becomes (frame ~600 MiB) + (one slice ~50 MiB) ≈ 650 MiB.

    Why a generator that probes the schema from the first slice rather than
    `frame.schema`: polars→arrow type mappings depend on whether the column
    is nullable, has a tz, or is a Decimal — easier to take the schema from
    an actual conversion than to translate dtype-by-dtype. Mirrors the
    pattern used by `ingest._ingest_transactions` for the chunked JSONL
    Bronze write.

    `overwrite_schema=True` is required for warm-cache idempotency under the
    rust engine (Trap G). Polars→arrow conversion produces `large_string`
    and tz-aware `timestamp[us, tz=UTC]`; delta-rs persists them as `string`
    and `timestamp[us]` (Trap B — tz label is stripped on write regardless
    of engine). On the next overwrite the rust engine sees the on-disk
    schema differ from the incoming and refuses unless `overwrite_schema`
    is set.
    """
    parent = Path(path).parent
    if not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)

    slices = frame.iter_slices(n_rows=_WRITE_SLICE_ROWS)
    try:
        first_slice = next(slices)
    except StopIteration:
        # Empty frame — preserve previous behaviour by writing the empty
        # table once. Stage 1 never hits this branch.
        write_deltalake(
            str(path),
            data=frame.to_arrow(),
            mode=mode,
            partition_by=partition_by,
            engine="rust",
            overwrite_schema=(mode == "overwrite"),
        )
        return

    first_table = first_slice.to_arrow()
    arrow_schema = _strip_timestamp_tz(first_table.schema)
    first_batches = [
        _cast_batch_to_schema(b, arrow_schema) for b in first_table.to_batches()
    ]
    del first_slice, first_table

    def _all_batches() -> Iterator[pa.RecordBatch]:
        for batch in first_batches:
            yield batch
        first_batches.clear()
        for slice_df in slices:
            slice_table = slice_df.to_arrow()
            for batch in slice_table.to_batches():
                yield _cast_batch_to_schema(batch, arrow_schema)
            del slice_df, slice_table

    write_deltalake(
        str(path),
        data=_all_batches(),
        schema=arrow_schema,
        mode=mode,
        partition_by=partition_by,
        engine="rust",
        overwrite_schema=(mode == "overwrite"),
    )


def read_delta_lazy(path: str | Path) -> pl.LazyFrame:
    """Read a Delta table as a polars LazyFrame."""
    return pl.scan_delta(str(path))


def advise_dontneed(delta_dir: str | Path) -> None:
    """Drop kernel page cache for parquet files under a Delta directory.

    On Docker-for-Mac (osxfs/virtiofs) bind-mount file cache is charged to
    the cgroup (Trap H) — without this advisory, parquet files read once
    during a stage continue to occupy ~hundreds of MiB of cgroup-accounted
    file cache for the rest of the run. The Linux scoring host largely
    ignores bind-mount file cache for cgroup accounting, so this is a quiet
    no-op there. Either way, mirrors the existing posix_fadvise pattern in
    `ingest._iter_jsonl_chunks`. Errors are swallowed: nothing in the
    pipeline depends on the advisory succeeding.
    """
    try:
        for p in Path(delta_dir).rglob("*.parquet"):
            fd = os.open(str(p), os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
    except (AttributeError, OSError):
        pass
