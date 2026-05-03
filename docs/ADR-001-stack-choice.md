# ADR-001 — Stack choice: Polars + delta-rs over PySpark

**Status:** Accepted (Stage 1)
**Decision date:** 2026-05-02
**Supersedes:** none
**Superseded by:** none

---

## Decision (one line)

Use **Polars (lazy/streaming) + `deltalake` (Rust) + Pandera + DuckDB** for
Stages 1–2. Keep PySpark in the base image untouched — it is a permitted
fallback, not the active engine.

---

## Context

The DE Challenge container runs under hard, non-negotiable constraints:

- **2 GB RAM**, **2 vCPU**
- **30 min** total wall-clock budget for all stages
- **`--read-only`** root filesystem with a single 512 MB tmpfs at `/tmp`
- **`--network=none`**
- Stage 1 input: 80 k customers (CSV) + 100 k accounts (CSV) + 1 M transactions (JSONL ≈ 600 MB)
- Stage 2 input scales transactions to ≈ 3 M rows and injects DQ noise
- Stage 3 adds a streaming SLA: 5-min lag from `/data/stream/*.jsonl` arrival to Gold update

The challenge base image (`nedbank-de-challenge/base:1.0`) ships
**both** PySpark + delta-spark **and** the building blocks for a
non-Spark stack (Python 3.11, pyarrow, pandas, duckdb, pyyaml). This ADR
records the choice between them.

---

## Decision drivers

1. **Memory headroom on a 2 GB cap.** PySpark's `local[2]` driver allocates
   ~700 MB of heap (≈ 512 MB driver + ≈ 256 MB executor) before reading the
   first row, plus JVM permgen / metaspace. Subtracting Python / OS overhead
   (~150 MB), user data has under 300 MB to work in. The 1 M-row Silver
   transactions frame is ~500 MB materialised. A polars-only stack moves
   the entire memory budget into the same address space as the data, leaving
   ~1.5 GB of true working room.

2. **JVM cold-start vs the 30-min budget.** A Spark session takes
   ~5 s to initialise, ~3 s per `read.format("delta").load(...)`, and
   ~10 s for the first action (Catalyst plan + codegen). For a Stage 1
   pipeline that does ingest + 3 Silver writes + 3 Gold writes + 3
   validation queries, the JVM/Catalyst overhead alone is ~45 s.
   The Polars + delta-rs + DuckDB stack does the same work in ~15 s
   wall clock, with no JVM and no Catalyst codegen path. That is not a
   performance flex — it is headroom against the 30-min cliff for Stage 2's
   3× data, Stage 2's DQ logic, and unforeseen scorer overhead.

3. **Streaming Bronze writes.** The 1 M-row JSONL is ~600 MB raw. Reading
   it eagerly into a single DataFrame and then writing Delta peaks at
   ~1.6 GB transient — leaves no headroom for the rest of the pipeline.
   The Polars stack does Bronze ingest as a chunked iterator (100 k
   lines/chunk → `pl.read_ndjson` → `with_columns(audit_expressions)` →
   per-chunk `to_arrow().to_batches()` → `write_deltalake(engine="rust",
   data=Iterator[RecordBatch])`). No `/tmp` parquet detour, no full-table
   materialisation in flight; peak resident at ingest is ~80 MB. The Silver
   write follows the same pattern via [io_utils.write_delta](../pipeline/io_utils.py),
   feeding sliced `RecordBatch` from `frame.iter_slices(...)` into the
   rust engine to avoid the doubled `frame.to_arrow()` copy that the
   previous single-Table form forced. Spark Structured Streaming would
   solve this on a different stack, but for batch JSONL → Delta the
   chunked-iterator path is far simpler and avoids the JVM checkpoint
   state.

4. **DuckDB for the validation harness.** The three scored queries are
   ordinary SQL; running them against the Gold tables we just wrote is
   pure analytics, not part of the pipeline's transformation logic. DuckDB
   is in the base image, embedded (single in-process connection — no
   network listener), and has no JVM startup cost. We register the Gold
   Arrow tables as DuckDB views and run the queries verbatim from
   `docs/validation_queries.sql`. The harness adds ~1 s to the run.

5. **Pandera for type contracts.** Every layer boundary writes a Pandera
   schema check before the Delta write. `pandera.polars` is native Polars;
   `pandera.pyspark` exists but is heavier and forces a Python ↔ JVM
   round-trip per row group. The Pandera check is the difference between
   "fail at write with the offending column name" and "fail in scoring with
   a schema-conformance error and no diagnostic".

---

## Rejected alternatives

### A. PySpark + delta-spark (the base-image default)

Rejected primarily on the **memory math** (~300 MB user-heap on a 2 GB cap)
and the **JVM cold-start tax** (~45 s of fixed overhead in a 30-min run).
Both are survivable in isolation; together with Stage 2's 3× data and DQ
overhead, they leave too little operational margin.

Secondary reasons:

- Spark's strength — distributed execution, optimistic concurrency on
  Delta, MLlib — is irrelevant on a single 2-vCPU container.
- Spark's `local[2]` mode has known issues with shuffle buffers and
  /tmp pressure under tight memory caps. Tuning `spark.sql.shuffle.partitions`,
  `spark.driver.memory`, and `spark.memory.fraction` to fit a 2 GB cap is
  possible but eats engineering time better spent on data quality.

### B. pandas + pyarrow + delta-rs

Rejected on **dedup memory** and **decimal correctness**.

- pandas dedup of a 1 M-row frame keyed on `transaction_id` peaks at
  ~1.2 GB transient (2× the frame for the hash index). Polars dedup on
  the same frame peaks at ~600 MB (Polars uses a single-pass radix
  partition for `unique(subset=...)`). At Stage 2's 3 M rows pandas
  blows the 2 GB cap; polars stays under.
- pandas' `Decimal` story is convoluted (`object` dtype, slow comparisons,
  no native arrow `decimal128` support until 2.x with caveats). Polars
  has first-class `Decimal(precision, scale)` via the Rust core (gated
  behind the feature flag — see Trap A in the README).

### C. Polars + parquet (no Delta layer)

Rejected because the **scorer reads via `delta.load` / `delta_scan`**, not
plain parquet. Without `_delta_log/` the Gold output is invisible to the
scoring harness. delta-rs is the cheapest way to satisfy this — it adds
~30 ms per write call and produces Delta tables compatible with both
PySpark and DuckDB (the latter via the `delta` extension or the
arrow-register workaround we use offline).

### D. DuckDB for the entire pipeline

Tempting — DuckDB has excellent CSV/JSON readers, native Decimal, native
Delta reader (with `INSTALL delta`), and the validation queries would be
inline with the rest of the work. Rejected because:

- `INSTALL delta` requires network access; the scorer runs `--network=none`
  and the base image does not pre-bundle the Delta extension.
- DuckDB's `INSERT INTO` against a Delta table is via the same extension —
  we cannot **write** Delta from DuckDB offline.
- DuckDB is excellent at SQL but does not have first-class Pandera support;
  the schema-contract layer would need to bounce through Arrow → polars →
  Pandera anyway.

We use DuckDB **read-only** in the validation harness (where its SQL
ergonomics are best) and Polars + delta-rs for everything that has to
**write**.

---

## Consequences

### Positive

- **Pipeline runtime ~15 s** on the Stage 1 dataset under the prod-flag
  container, leaving ~29 min of headroom for Stage 2's 3× data and DQ work.
- **Peak resident memory ≤ 1.6 GB** measured via `docker stats` during
  fact_transactions provisioning, which has the largest working set.
  Stage 2 will need an out-of-core dedup path on a 3 M-row Silver
  transactions frame; the streaming-write Bronze pattern is already in
  place to support this.
- **No JVM cold start** — pipeline-debug iteration is fast enough to do
  printf-based debugging inside the container without `time`-amortising
  the startup.
- **Pandera contract at every boundary.** Every Bronze, Silver, and Gold
  write goes through `schema.validate(df)` first; off-contract Delta tables
  are impossible to ship.
- **Validation harness runs offline.** No `INSTALL delta`, no network
  required for the DuckDB queries; we register Arrow tables read via
  `pl.read_delta` as views.

### Negative

- **Single-node only.** Polars cannot scale horizontally. If Stage 4
  requires distributed execution, the entire transformation layer is
  rewritten — though the schema, config, and Pandera contracts survive
  intact since they are framework-agnostic.
- **Single-writer Delta tables only.** delta-rs's optimistic concurrency
  is weaker than Spark's; concurrent writers to the same Delta path can
  produce conflicts that delta-rs surfaces as exceptions rather than
  retrying. Our pipeline is a single-writer batch job, so this is fine
  today, but a Stage 3 streaming path that runs concurrently with the
  batch path needs to write to a different Delta location.
- **Pandera + Polars compatibility surface is narrower than Pandera +
  pandas.** A few Pandera features (`Index`, custom `Check` registration)
  work differently or not at all in `pandera.polars`. The Stage 1 schemas
  use only column-level types/nullability/uniqueness, which are fully
  supported.
- **Eight trap-class issues** surfaced during the Stage 1 build (Phases
  3–7; see README § Locked-in traps). Trap C is struck (no /tmp parquet
  on the hot path anymore), but Phase 7's memory-optimisation pass added
  three new ones: G (rust engine + `overwrite_schema`), H (Mac
  bind-mount cache cgroup-charged), and I (streaming `RecordBatch` write
  path preserves tz unless explicitly cast). All are working around
  limitations of `polars==0.20.31` or `deltalake==0.15.3`, not of the
  architecture itself. Upgrading to polars 1.x and deltalake 0.18+ in
  Stage 2 may eliminate several of them; we will pin upgrade versions to
  a Pandera-compatible set.

---

## Migration triggers

We expect to revisit this decision when **any one** of the following holds.
Each trigger is a measurable signal, not an opinion call.

1. **Single-node Polars dedup of >5 M rows blows /tmp.** The 1 M-row
   transactions dedup uses a /tmp parquet at ~250 MB peak; on the 512 MB
   tmpfs we are at 50 % utilisation today. Stage 2 (3 M rows) will hit
   ~750 MB → already over the cap. We will migrate the transactions dedup
   to **DuckDB on the temp parquet** before falling back to Spark, since
   DuckDB has a true out-of-core hash dedup that polars 0.20.x does not.
   If Stage 4+ pushes >5 M rows of *anything else* (customers, accounts),
   the same DuckDB pattern applies. Spark only enters the picture if the
   working set genuinely exceeds 2 GB and a single node cannot hold it.

2. **DQ logic outgrows declarative Pandera.** Stage 1's data is clean.
   Stage 2's DQ rules are still expressible as per-row predicates (Pandera
   `Check` objects, plus `dq_flag` population in `transform.py`). If
   Stage 3 introduces **iterative reconciliation** — e.g. a
   transactions-vs-balance reconciliation loop, or a graph-walk over
   account relationships — Pandera's declarative model does not fit and
   we move that step to Spark MLlib or a graph engine. The boundary is:
   "can the DQ rule be expressed as `f(row) → bool` or `f(row) → flag`?"
   If yes, stay on Pandera. If it requires a reduction over related rows,
   migrate.

3. **Streaming SLA tightens below 60 s.** Stage 3's spec is a 5-min lag
   (300 s). Polars + delta-rs' Bronze-write path takes ~5 s for a
   100 k-row micro-batch — comfortably inside 300 s. If a Stage 4 mobile
   product team requires sub-60 s end-to-end, a polling-based batch
   architecture will not fit and the pipeline becomes Spark Structured
   Streaming, which has a true continuous-mode trigger. The migration
   point is the spec, not a code limit.

4. **Multi-writer concurrency required on the same Delta table.**
   delta-rs handles single-writer scenarios well; concurrent writers
   require careful coordination. If Stage 4 mandates that the batch
   pipeline and the streaming pipeline write to the **same** Gold Delta
   table simultaneously, Spark's optimistic concurrency layer is the
   right tool. Today they write to different paths (`/data/output/gold/`
   vs `/data/output/stream_gold/`), so this trigger is dormant.

5. **MLlib needed downstream.** Polars has no distributed ML; the only
   in-process ML option is scikit-learn on a `.to_pandas()` materialise.
   If the Gold layer becomes the input to a Spark ML pipeline, the team
   that owns the ML side has the Spark dependency anyway, and we hand
   off Delta tables — no migration on our side. If our pipeline itself
   has to *fit ML models*, Spark replaces Polars at that layer.

---

## Implementation notes

- **Pinning.** `polars==0.20.31`, `deltalake==0.15.3`,
  `pandera[polars]==0.20.4`, `numpy<2` (deltalake 0.15 is incompatible with
  numpy 2.x). These pins are deliberate; "latest" is not in the freeze.
- **Polars Decimal feature flag.** `ENV POLARS_ACTIVATE_DECIMAL=1` in the
  Dockerfile. Without it, every `cast(pl.Decimal(...))` is a silent no-op
  (Trap A). Do not remove.
- **Pandera + Polars Decimal** has a buggy false-pass on Float64 → Decimal
  in `pandera==0.20.4`; the env var above is the only line of defence.
- **Delta tz preservation.** `deltalake==0.15.3`'s rust engine strips UTC
  tz labels when given a single `pyarrow.Table` but PRESERVES them when
  given an `Iterator[RecordBatch]` (Trap I). Both Bronze and Silver
  writes use the iterator form; [io_utils._strip_timestamp_tz](../pipeline/io_utils.py)
  + `_cast_batch_to_schema` re-cast each batch to a tz-stripped schema
  before yielding to keep on-disk parquet uniformly `timestamp[us]`.
  Values on disk are still UTC microseconds since epoch — the Gold schema
  spec accepts this; DuckDB and PySpark readers receive correct UTC values.

---

*Author: pipeline owner. Review: data engineering team. Next review: Stage 2
kickoff or earlier if a migration trigger fires.*
