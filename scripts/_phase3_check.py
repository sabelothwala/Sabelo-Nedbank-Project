"""Phase 3 verification — runs Bronze ingestion then asserts the contract.

Not part of the pipeline. Mounted at runtime via -v $PWD/scripts:/app/scripts:ro
and invoked as the container's command override.

Checks:
  1. run_ingestion() exits cleanly.
  2. /data/output/bronze/{customers,accounts,transactions} each contain a
     valid Delta table (_delta_log present).
  3. Row counts match the source line counts (80,000 / 100,000 / 1,000,000).
  4. All 4 audit columns present and non-null on every row.
  5. transactions has merchant_subcategory column with Utf8 dtype, all-null.
  6. transactions retains the flattened location_*/metadata_* columns.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import polars as pl

from pipeline.config import load_config
from pipeline.ingest import run_ingestion

AUDIT_COLS = ("_ingested_at", "_source_system", "_batch_id", "_source_row_hash")
EXPECTED_ROWS = {"customers": 80_000, "accounts": 100_000, "transactions": 1_000_000}


def _assert(cond: bool, msg: str) -> None:
    status = "OK  " if cond else "FAIL"
    print(f"  {status}  {msg}")
    if not cond:
        raise AssertionError(msg)


def check_table(name: str, path: Path) -> None:
    print(f"── {name} ──")
    _assert(path.is_dir(), f"{path} exists")
    _assert((path / "_delta_log").is_dir(), f"{path}/_delta_log exists")

    df = pl.scan_delta(str(path)).collect()
    rows = df.height
    _assert(rows == EXPECTED_ROWS[name],
            f"row count {rows} == {EXPECTED_ROWS[name]}")

    for col in AUDIT_COLS:
        _assert(col in df.columns, f"audit col present: {col}")
        nulls = df[col].null_count()
        _assert(nulls == 0, f"audit col {col} has 0 nulls (got {nulls})")

    # source_system stamped from config — sanity check
    distinct_ss = df["_source_system"].unique().to_list()
    _assert(len(distinct_ss) == 1,
            f"single _source_system value (got {distinct_ss})")

    if name == "transactions":
        # merchant_subcategory: present, Utf8, all-null in Stage 1
        _assert("merchant_subcategory" in df.columns,
                "transactions has merchant_subcategory column")
        msc_dtype = df.schema["merchant_subcategory"]
        _assert(msc_dtype == pl.Utf8,
                f"merchant_subcategory dtype is Utf8 (got {msc_dtype})")
        _assert(df["merchant_subcategory"].null_count() == rows,
                "merchant_subcategory all-null in Stage 1")

        # Flattened nested keys present
        flat_cols = (
            "location_province", "location_city", "location_coordinates",
            "metadata_device_id", "metadata_session_id", "metadata_retry_flag",
        )
        for c in flat_cols:
            _assert(c in df.columns, f"transactions has flattened col: {c}")

        # Original nested struct cols dropped
        _assert("location" not in df.columns, "raw location struct dropped")
        _assert("metadata" not in df.columns, "raw metadata struct dropped")

    print(f"   sample audit row: {df.select(AUDIT_COLS).row(0)}")


def main() -> int:
    cfg = load_config()
    bronze_root = Path(cfg["output"]["bronze_path"])

    print(f"resolved config: {cfg['_resolved_config_path']}")
    print(f"bronze root:     {bronze_root}")
    print()
    print("── Running run_ingestion() ──")
    t0 = time.monotonic()
    run_ingestion()
    elapsed = time.monotonic() - t0
    print(f"   ingest elapsed: {elapsed:.1f}s")
    print()

    for name in ("customers", "accounts", "transactions"):
        check_table(name, bronze_root / name)
        print()

    print("ALL PHASE 3 CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
