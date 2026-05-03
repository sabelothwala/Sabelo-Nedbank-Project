"""Phase 4 verification — runs Bronze ingest then Silver transform, asserts the
contract. Mounted at runtime; not part of the image.

Checks:
  1. run_ingestion + run_transformation exit cleanly.
  2. Each Silver Delta exists with valid _delta_log.
  3. Row counts match (no rows dropped beyond legitimate dedup; Stage 1 has
     no dupes so rows_in == rows_out).
  4. Type contracts:
        customers.dob:                        Date
        customers.risk_score:                 Int32
        accounts.open_date / last_activity:   Date
        accounts.credit_limit / current_bal:  Decimal(18,2)
        transactions.transaction_date:        Date
        transactions.transaction_timestamp:   Datetime("us")
        transactions.amount:                  Decimal(18,2)
  5. Audit columns preserved (4 cols, 0 nulls).
  6. transactions.merchant_subcategory all-null Utf8.
  7. transactions.dq_flag all-null Utf8.
  8. transactions.currency all 'ZAR'.
  9. Referential integrity: 0 orphans both directions (clean Stage 1 data).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import polars as pl

from pipeline.config import load_config
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation, _check_referential_integrity

AUDIT_COLS = ("_ingested_at", "_source_system", "_batch_id", "_source_row_hash")
EXPECTED_ROWS = {"customers": 80_000, "accounts": 100_000, "transactions": 1_000_000}


def _assert(cond: bool, msg: str) -> None:
    status = "OK  " if cond else "FAIL"
    print(f"  {status}  {msg}")
    if not cond:
        raise AssertionError(msg)


def check_customers(silver_root: Path) -> None:
    print("── silver/customers ──")
    path = silver_root / "customers"
    _assert((path / "_delta_log").is_dir(), "_delta_log present")
    df = pl.read_delta(str(path))
    _assert(df.height == EXPECTED_ROWS["customers"], f"rows={df.height}")
    s = df.schema
    _assert(s["dob"] == pl.Date, f"dob is Date (got {s['dob']})")
    _assert(s["risk_score"] == pl.Int32, f"risk_score is Int32 (got {s['risk_score']})")
    for col in AUDIT_COLS:
        _assert(df[col].null_count() == 0, f"audit {col} non-null")


def check_accounts(silver_root: Path) -> None:
    print("── silver/accounts ──")
    path = silver_root / "accounts"
    _assert((path / "_delta_log").is_dir(), "_delta_log present")
    df = pl.read_delta(str(path))
    _assert(df.height == EXPECTED_ROWS["accounts"], f"rows={df.height}")
    s = df.schema
    _assert(s["open_date"] == pl.Date, f"open_date is Date (got {s['open_date']})")
    _assert(s["last_activity_date"] == pl.Date, f"last_activity_date is Date")
    _assert(s["credit_limit"] == pl.Decimal(18, 2),
            f"credit_limit Decimal(18,2) (got {s['credit_limit']})")
    _assert(s["current_balance"] == pl.Decimal(18, 2), "current_balance Decimal(18,2)")
    # nullable cols: empty CSV strings should have become nulls
    cl_nulls = df["credit_limit"].null_count()
    _assert(cl_nulls > 0, f"credit_limit has nulls for non-CREDIT accts (got {cl_nulls})")
    print(f"   credit_limit nulls={cl_nulls} (non-CREDIT accounts)")
    for col in AUDIT_COLS:
        _assert(df[col].null_count() == 0, f"audit {col} non-null")


def check_transactions(silver_root: Path) -> None:
    print("── silver/transactions ──")
    path = silver_root / "transactions"
    _assert((path / "_delta_log").is_dir(), "_delta_log present")
    df = pl.read_delta(str(path))
    _assert(df.height == EXPECTED_ROWS["transactions"], f"rows={df.height}")

    s = df.schema
    _assert(s["transaction_date"] == pl.Date, f"transaction_date Date")
    _assert(s["transaction_timestamp"] == pl.Datetime("us"),
            f"transaction_timestamp Datetime(us) (got {s['transaction_timestamp']})")
    _assert(s["amount"] == pl.Decimal(18, 2), f"amount Decimal(18,2)")
    _assert(s["currency"] == pl.Utf8, "currency Utf8")
    _assert(s["dq_flag"] == pl.Utf8, f"dq_flag Utf8 (got {s['dq_flag']})")
    _assert(s["merchant_subcategory"] == pl.Utf8, "merchant_subcategory Utf8")

    # transaction_id uniqueness (Pandera already checked; double-verify)
    _assert(df["transaction_id"].n_unique() == df.height, "transaction_id unique")

    # currency standardisation
    distinct_ccy = sorted(df["currency"].unique().to_list())
    _assert(distinct_ccy == ["ZAR"], f"currency ∈ {{ZAR}} (got {distinct_ccy})")

    # all-null contracts
    _assert(df["dq_flag"].null_count() == df.height, "dq_flag all-null Stage 1")
    _assert(df["merchant_subcategory"].null_count() == df.height,
            "merchant_subcategory all-null Stage 1")

    for col in AUDIT_COLS:
        _assert(df[col].null_count() == 0, f"audit {col} non-null")

    # transaction_timestamp sanity: should equal transaction_date + transaction_time
    sample = df.head(1)
    print(f"   sample: date={sample['transaction_date'][0]} "
          f"time={sample['transaction_time'][0]} "
          f"ts={sample['transaction_timestamp'][0]}")


def check_ri(cfg) -> None:
    print("── referential integrity ──")
    res = _check_referential_integrity(cfg)
    _assert(res["accounts"] == 0,
            f"accounts→customers orphans = 0 (got {res['accounts']})")
    _assert(res["transactions"] == 0,
            f"transactions→accounts orphans = 0 (got {res['transactions']})")


def main() -> int:
    cfg = load_config()
    bronze_root = Path(cfg["output"]["bronze_path"])
    silver_root = Path(cfg["output"]["silver_path"])

    if not (bronze_root / "transactions" / "_delta_log").is_dir():
        print("── Bronze missing — running ingestion ──")
        t0 = time.monotonic()
        run_ingestion()
        print(f"   ingest elapsed: {time.monotonic() - t0:.1f}s")
        print()

    print("── Running run_transformation() ──")
    t0 = time.monotonic()
    run_transformation()
    elapsed = time.monotonic() - t0
    print(f"   transform elapsed: {elapsed:.1f}s")
    print()

    check_customers(silver_root)
    print()
    check_accounts(silver_root)
    print()
    check_transactions(silver_root)
    print()
    check_ri(cfg)

    print()
    print("ALL PHASE 4 CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
