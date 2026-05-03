"""DuckDB validation harness.

Runs the three scored validation queries from docs/validation_queries.sql
against the Gold layer. This is our internal safety net — the scoring system
runs equivalent queries separately. A failure here exits the pipeline non-zero
*before* the scorer sees the output, which is strictly preferable to shipping
broken Delta tables.

Why arrow-register, not delta_scan
----------------------------------
DuckDB 0.10's `delta` extension is NOT bundled in the base image and the
container runs with `--network=none`, so `INSTALL delta; LOAD delta;` fails
with an HTTP error. Verified on this image:
    INSTALL delta failed: Failed to download extension "delta" at URL ...

Workaround: read the three Gold Delta tables via Polars (which uses delta-rs,
not the DuckDB extension), convert each to a PyArrow table, and register the
arrow tables as DuckDB views. The validation SQL is unchanged except for
substituting the registered view names for `delta_scan(...)` — same query
results, no extension required.

Checks performed before exiting zero
------------------------------------
  Q1  fact_transactions GROUP BY transaction_type
       -> exactly 4 rows, no NULLs in transaction_type
  Q2  dim_accounts LEFT JOIN dim_customers ON customer_id
       -> unlinked_accounts == 0  (zero tolerance — GAP-026 boundary)
  Q3  province distribution from dim_accounts JOIN dim_customers
       -> exactly 9 rows, all canonical SA province names

Each failure raises ValueError with the offending query name and a one-line
diagnostic, so the pipeline's stderr tells you exactly which contract broke.
"""

from __future__ import annotations

import logging

import duckdb
import polars as pl

from pipeline.config import load_config
from pipeline.schemas import SA_PROVINCES, TRANSACTION_TYPES

log = logging.getLogger("pipeline.validate")


def _register_gold(con: duckdb.DuckDBPyConnection, gold_path: str) -> None:
    """Read each Gold Delta with Polars, hand the Arrow table to DuckDB.

    `register` exposes the arrow table as a SQL view — zero-copy, no IPC, no
    materialise into DuckDB's storage. Memory cost is roughly the size of the
    arrow table (~50 MB for fact_transactions at 1M rows).
    """
    con.register("dim_customers",    pl.read_delta(f"{gold_path}/dim_customers").to_arrow())
    con.register("dim_accounts",     pl.read_delta(f"{gold_path}/dim_accounts").to_arrow())
    con.register("fact_transactions", pl.read_delta(f"{gold_path}/fact_transactions").to_arrow())


def _check_q1_transaction_volume(con: duckdb.DuckDBPyConnection) -> None:
    """Q1: exactly 4 rows, one per transaction type, no NULL transaction_type."""
    rows = con.execute(
        """
        SELECT
            transaction_type,
            COUNT(*)                                     AS record_count,
            SUM(amount)                                  AS total_amount,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
        FROM fact_transactions
        GROUP BY transaction_type
        ORDER BY transaction_type
        """
    ).fetchall()
    log.info("Q1 transaction_volume: %s", rows)

    types = [r[0] for r in rows]
    if any(t is None for t in types):
        raise ValueError(f"Q1 failed: NULL transaction_type in fact_transactions. rows={rows}")
    if len(rows) != 4:
        raise ValueError(
            f"Q1 failed: expected 4 transaction_type groups, got {len(rows)}. rows={rows}"
        )
    expected = set(TRANSACTION_TYPES)
    actual = set(types)
    if actual != expected:
        raise ValueError(
            f"Q1 failed: transaction_type set mismatch. "
            f"expected={sorted(expected)} actual={sorted(actual)}"
        )


def _check_q2_unlinked_accounts(con: duckdb.DuckDBPyConnection) -> None:
    """Q2: unlinked_accounts must be 0 (zero tolerance — GAP-026 boundary)."""
    (count,) = con.execute(
        """
        SELECT COUNT(*)
        FROM dim_accounts a
        LEFT JOIN dim_customers c ON a.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    ).fetchone()
    log.info("Q2 unlinked_accounts: %d", count)

    if count != 0:
        raise ValueError(
            f"Q2 failed: {count} unlinked accounts in dim_accounts (zero tolerance). "
            "Check that dim_accounts.customer_id was renamed from customer_ref (GAP-026) "
            "and that all Silver accounts have a matching customer."
        )


def _check_q3_province_distribution(con: duckdb.DuckDBPyConnection) -> None:
    """Q3: exactly 9 rows, all canonical SA province names."""
    rows = con.execute(
        """
        SELECT c.province, COUNT(DISTINCT a.account_id) AS account_count
        FROM dim_accounts a
        JOIN dim_customers c ON a.customer_id = c.customer_id
        GROUP BY c.province
        ORDER BY c.province
        """
    ).fetchall()
    log.info("Q3 province_distribution: %s", rows)

    if len(rows) != 9:
        raise ValueError(
            f"Q3 failed: expected 9 SA provinces, got {len(rows)}. rows={rows}"
        )
    actual = {r[0] for r in rows}
    expected = set(SA_PROVINCES)
    if actual != expected:
        missing = expected - actual
        extra = actual - expected
        raise ValueError(
            f"Q3 failed: province set mismatch. missing={sorted(missing)} extra={sorted(extra)}"
        )


def run_validation() -> None:
    cfg = load_config()
    gold_path = cfg["output"]["gold_path"]
    log.info("validate start gold_path=%s", gold_path)

    con = duckdb.connect()
    try:
        _register_gold(con, gold_path)
        _check_q1_transaction_volume(con)
        _check_q2_unlinked_accounts(con)
        _check_q3_province_distribution(con)
    finally:
        con.close()

    log.info("validate done — all 3 queries passed")
