"""Read 100 rows of each Silver Delta table and dump schema + samples.

Run inside the container with:
  docker run --rm \
    -v "$PWD/output:/data/output" \
    -v "$PWD/scripts:/app/scripts" \
    --entrypoint python nedbank-de-stage1:dev /app/scripts/_silver_inspect.py
"""

from __future__ import annotations

import polars as pl

paths = {
    "customers":    "/data/output/silver/customers",
    "accounts":     "/data/output/silver/accounts",
    "transactions": "/data/output/silver/transactions",
}

for name, path in paths.items():
    print(f"\n========== silver.{name} ==========")
    df = pl.read_delta(path)
    print(f"rows: {df.height}")
    print("schema:")
    for col, dtype in df.schema.items():
        print(f"  {col:24s} {dtype}")
    print("head(5):")
    print(df.head(5))
