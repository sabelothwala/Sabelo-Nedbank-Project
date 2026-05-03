# Nedbank DE Challenge — Stage 1 Pipeline

A containerised medallion pipeline (Bronze → Silver → Gold) for the Nedbank
Data Engineering Challenge 2026 — Stage 1. Implements all three Gold tables
(`dim_customers`, `dim_accounts`, `fact_transactions`) and an internal DuckDB
harness that runs the three scored validation queries before exit.

The pipeline runs end-to-end in **~15 s** on a 2 GB / 2 vCPU container with
`--read-only --network=none` against the Stage 1 dataset (80 k customers,
100 k accounts, 1 M transactions).

---

## Stack

**Polars (lazy/streaming)** + **`deltalake` (Rust)** + **Pandera** + **DuckDB**.

PySpark is in the base image and untouched — see `docs/ADR-001-stack-choice.md`
for why it is dormant rather than active.

---

## Layout

```
Dockerfile                  extends nedbank-de-challenge/base:1.0
requirements.txt            polars==0.20.31, deltalake==0.15.3, pandera[polars]==0.20.4, numpy<2
pipeline/
  __init__.py
  run_all.py                orchestrator (Docker CMD entry point)
  ingest.py                 Bronze: CSV / JSONL → typed-as-bytes Delta + audit
  transform.py              Silver: typed, deduplicated, standardised
  provision.py              Gold:   star-schema dims + fact, surrogate keys
  validate.py               DuckDB harness for the three scored queries
  schemas.py                Pandera contracts for every layer boundary
  config.py                 YAML loader, PIPELINE_CONFIG env var
  io_utils.py               Delta read/write + audit-column factory
config/
  pipeline_config.yaml      runtime config (mounted at /data/config in scoring)
docs/
  ADR-001-stack-choice.md   Polars vs Spark; migration triggers
  data_dictionary.md        source-of-truth for source columns + GAP-026
  output_schema_spec.md     Gold contract (15/11/9 fields, ordering, GAP-026)
  validation_queries.sql    the three scored queries
scripts/
  _phase[2-5]_check.py      out-of-image verification harnesses
```

---

## Running locally (mirrors the scoring constraints)

```bash
# Build (base image must already be built from infrastructure/Dockerfile.base)
docker build -t nedbank-de-stage1:dev .

# E2E run with the prod-equivalent security flags
rm -rf output && mkdir -p output
docker run --rm \
  -m 2g --memory-swap=2g --cpus=2 \
  --read-only --tmpfs /tmp:rw,size=512m \
  --network=none \
  -v "$PWD/data:/data/input:ro" \
  -v "$PWD/config:/data/config:ro" \
  -v "$PWD/output:/data/output" \
  --name de_e2e \
  nedbank-de-stage1:dev
```

Exit 0 with the three validation query results on stderr indicates a clean
Stage 1 submission. To watch peak memory, run `docker stats --no-stream de_e2e`
in a second terminal while the pipeline is alive.

---

## POPIA §19 audit trail

POPIA §19 requires that personal-data processing be auditable: who processed
the data, when, in which run, and over which source bytes. Every Bronze write
appends four columns; they are preserved through Silver, then dropped in Gold
(except `_ingested_at`, which is renamed to `fact_transactions.ingestion_timestamp`).

| Column             | Type             | Purpose                                                           |
|--------------------|------------------|-------------------------------------------------------------------|
| `_ingested_at`     | `Datetime(us,UTC)` | wall-clock UTC timestamp when the row entered Bronze            |
| `_source_system`   | `Utf8`             | logical source name from `cfg.sources.<src>.source_system`      |
| `_batch_id`        | `Utf8`             | per-run id `yyyyMMddTHHMMSSZ-<6hex>`, sortable by run            |
| `_source_row_hash` | `Utf8`             | xxhash64 over the source columns only (audit cols excluded)     |

The four expressions are emitted in a single `with_columns(...)` call inside
[io_utils.audit_expressions](pipeline/io_utils.py), so the hash sees the
pre-audit frame and never includes audit metadata in its own input.

**Why xxhash64 (UInt64 → Utf8) and not sha256.** Drift detection is the use
case — we need to notice when a row's source bytes change, not resist a
cryptographic adversary. xxhash64 is the polars built-in (`Expr.hash`); a
pure-Python sha256 over the 1 M-row transactions frame costs ~30 s of wall
clock — over twice our entire pipeline runtime. Collision probability on 1 M
rows at 64 bits is ≈ 5 × 10⁻⁸; on a 100 M-row Stage-2-scale corpus it is
≈ 5 × 10⁻⁴, which is still acceptable for drift-detection purposes. If
crypto-strength hashing becomes a downstream requirement, swap the expression
in `audit_expressions` — call sites do not change.

**Where the columns live**:

```
Bronze:  written      (ingest.py)
Silver:  preserved    (transform.py — _restore_audit_tz on read)
Gold:    dropped      (provision.py — except _ingested_at → fact_transactions.ingestion_timestamp)
```

---

## Three-tier schema drift policy

Configured via `cfg.drift.policy`. Stage 1 ships with `quarantine` because
financial sources should never silently mutate their schema, and a hard fail
on every new column blocks the upstream team rather than the data team.

| Tier         | Behaviour                                                                      | When to use                          | Wired in Stage 1?         |
|--------------|--------------------------------------------------------------------------------|--------------------------------------|---------------------------|
| `strict`     | Pipeline exits non-zero on any unknown column (Pandera `strict=True`)          | Sandbox / fixture-driven test runs   | Yes (Bronze + Silver)     |
| `quarantine` | Unknown column + its rows diverted to `silver/_quarantine/<src>/`; in-band row kept with the unknown column dropped; WARN logged | **Default for production financial sources** | Stage 2 build-out — quarantine writer is the missing piece; Pandera `strict=True` catches the drift today |
| `autoevolve` | Unknown columns silently extend the Silver schema                              | Never used; included only because `delta-rs`'s mergeSchema parameter exists | No, intentionally   |

**Why `quarantine` is the POPIA-aligned default**. POPIA §13(2) requires
processing be necessary for an explicitly defined purpose. A new source
column is, by definition, not yet covered by a defined purpose. Letting it
silently propagate (`autoevolve`) processes data that has not been authorised.
A hard fail (`strict`) prevents downstream consumers from getting any rows at
all, which is operationally worse than getting the rows minus the new field.
Quarantine threads the needle: in-band rows are processed minus the unknown
column, the unknown column lands in a side path for a human to map, and the
WARN tells the data team that something needs a definition.

**What is wired today vs Stage 2**:

- **Wired (Stage 1):** Pandera `strict=True` on all three Bronze schemas and
  all three Silver schemas. Tier 3 (a missing required column) is enforced
  end-to-end — pipeline exits non-zero with the offending column name.
- **Stage 2 build-out:** the actual quarantine sink. Bronze ingest currently
  rejects unknown source columns at the Pandera step rather than diverting
  them. The `_quarantine/<src>/` writer is two functions in `io_utils.py`
  and a `policy=='quarantine'` branch in the Bronze write path; it is not in
  Stage 1 because Stage 1 has no drifted rows to quarantine.

---

## Anti-patterns avoided

These are decisions the alternative for which would have looked superficially
fine on Stage 1 data and broken at Stage 2 scale or on the scorer's harness.

- **No `.collect()` on the 1 M-row transactions frame in lazy chains.**
  `pipeline.provision._provision_fact_transactions` builds a single lazy
  plan (scan → sort → row_index → join × 2 → select) and `.collect()`s
  exactly once. The earlier eager version OOM-killed at exit 137 inside the
  2 GB cap; lazy + project-pushdown kept peak resident under 1 GB.

- **No `frame.to_arrow()` single-shot at write time.** [io_utils.write_delta](pipeline/io_utils.py)
  feeds `write_deltalake` an `Iterator[RecordBatch]` produced from
  `frame.iter_slices(n_rows=100_000)`. The earlier single-shot version held
  the polars frame (~600 MiB) AND a freshly-allocated arrow Table (~600 MiB)
  in memory simultaneously while delta-rs ran the write — a transient spike
  of ~1.2 GiB just for the materialised representations, on top of bind-mount
  page cache. The streaming form holds frame + one ~50 MiB batch in flight.
  Same write speed, much smaller peak. See Trap I for why each batch is
  re-cast through a tz-stripped schema.

- **No /tmp parquet on the hot path.** Both `_ingest_transactions` (Bronze)
  and `_transform_transactions` (Silver) used to round-trip through a
  `/tmp` parquet temp file. That intermediate lived on the 512 MB tmpfs and
  counted against the 2 GB cgroup, AND `pl.read_parquet` materialised an
  in-memory copy alongside the tmpfs file. Bronze now streams `RecordBatch`
  straight from chunked JSONL into `write_deltalake`; Silver is a single
  lazy plan over the Bronze Delta with no temp file at all. Trap C
  (decimal precision lost across `/tmp` parquet round-trips) is no longer
  reachable.

- **No bind-mount page cache hoarding.** On Docker-for-Mac the kernel
  charges bind-mount file cache to the container cgroup (Trap H). The
  pipeline calls `posix_fadvise(POSIX_FADV_DONTNEED)` on the JSONL input
  at the end of `_iter_jsonl_chunks`, on the Bronze parquet tree at the
  end of `transform`, and on Silver transactions at the end of `provision`.
  Linux scoring host: quiet no-op. Mac local: ~200 MiB of recovered
  headroom on the fact_transactions write.

- **No pandas anywhere.** Pandera-on-pandas would force a polars-to-pandas
  copy on every layer boundary; on the 1 M-row Silver that is ≈ 600 MB of
  transient memory plus a known dtype-coercion footgun on `Decimal(18,2)`.
  We use `pandera.polars` exclusively.

- **No JVM.** `deltalake` (Rust) writes Delta tables without a Spark session.
  Spark `local[2]` would consume ~700 MB of driver heap before the first row
  is read; on a 2 GB cap that leaves ~300 MB for user data, which is below
  the 1 M-row Silver materialise. ADR-001 has the full memory math.

- **No `monotonic_id` / DataFrame.with_row_index().cast() for surrogate keys.**
  Polars' `with_row_index` is partition-local — under any future
  parallelisation it produces non-deterministic SKs across runs. We use
  `pl.int_range(1, pl.len() + 1, dtype=pl.Int64)` after a stable
  `sort(natural_key)`. Same input → same SK every run.

- **No hash-to-bigint surrogate keys.** `sha2(natural_key, 256) → BIGINT`
  truncates to 64 bits, giving a non-trivial collision risk at Stage 2
  scale (~3 M rows). Row-number on a stable sort has zero collision risk.

- **No `INSTALL delta` for the validation harness.** The container runs with
  `--network=none`; `INSTALL` reaches out to extensions.duckdb.org. We read
  Gold via `pl.read_delta`, hand the Arrow tables to DuckDB via `con.register`,
  and run the validation SQL against the registered views.

---

## SCD Type 2 readiness on `dim_customers`

`dim_customers` ships in Stage 1 as a Type 1 dimension — one row per
`customer_id`, no history. The pieces required to evolve it to Type 2 in
Stage 2 are already in place:

1. **Audit metadata is preserved through Silver.** `_ingested_at`,
   `_batch_id`, and `_source_row_hash` survive the Bronze→Silver dedup. A
   Stage 2 SCD Type 2 build can compute `effective_from = _ingested_at` and
   detect changes via `_source_row_hash` without re-reading the source.

2. **The Gold surrogate-key strategy is SCD-compatible.** `customer_sk` is
   `row_number()` over a stable sort on `customer_id`. To switch to Type 2,
   the sort key extends to `(customer_id, _ingested_at)` and the SK becomes
   per-version rather than per-customer. The natural key (`customer_id`)
   stays stable; foreign keys from `fact_transactions.customer_sk` resolve
   to the version-of-record at fact-row insert time.

3. **The Pandera schema needs only two additions.** `effective_from` and
   `effective_to` (Date, nullable for the open version). The existing 9
   fields stay; field count goes from 9 → 11. Scoring schema-conformance
   check has to be revisited but the Stage 1 validation queries
   (Q1/Q2/Q3) keep working because they don't reference SCD columns.

What is **not** here yet: the Type 2 merge logic itself. Stage 2 will
`MERGE INTO dim_customers ... WHEN MATCHED AND _source_row_hash <> ... THEN UPDATE`
the previous open version's `effective_to` and insert a new open version.
That is a Delta-MERGE pattern; `deltalake==0.15.3` supports it.

---

## Locked-in traps (do not re-trip)

Eight traps surfaced during Phases 3–7. Each cost ~1 hour to diagnose. They
are documented here as much for the next author as for memory.

### Trap A — Polars 0.20.x Decimal is gated behind a feature flag

Without `ENV POLARS_ACTIVATE_DECIMAL=1` (set in [Dockerfile](Dockerfile)),
every `cast(pl.Decimal(...))` is a **silent no-op** — the column stays
`Float64`. Worse, `pandera==0.20.4` has a buggy false-pass on `Float64`-vs-
`Decimal`, which masks the bug entirely: `validate()` returns OK while the
on-disk Delta table is off-contract. If you ever see
`AssertionError: The return is expected to be of Decimal class` from
pandera, you have lost the env var.

### Trap B — `deltalake` round-trip drops UTC tz labels (read AND write)

Audit `_ingested_at` is written as `Datetime("us", "UTC")` but reads back as
`Datetime("us", None)`. Pandera (correctly) rejects this against the schema.
[transform.py](pipeline/transform.py) and
[provision.py](pipeline/provision.py) re-tag with
`pl.col("_ingested_at").dt.replace_time_zone("UTC")` on every Bronze→Silver
and Silver→Gold read.

The writer's behaviour on tz labels depends on the input form: a single
`pyarrow.Table` is implicitly stripped, but an `Iterator[RecordBatch]`
(used by the streaming Bronze ingest and the streaming `write_delta` —
see Trap I) preserves the tz unless it is explicitly cast away. Either
way the *values* on disk remain UTC microseconds since epoch. The Gold
spec only requires `TIMESTAMP` (not "with tz"), and the on-disk parquet
type is uniformly `timestamp[us]` after [io_utils._strip_timestamp_tz](pipeline/io_utils.py)
runs — DuckDB and PySpark readers receive correct UTC values.

### Trap C — *(struck — no longer applies)*

The earlier Silver `_transform_transactions` went through a `/tmp` parquet
intermediate where `Decimal(18, 2)` precision metadata was lost on the
read leg. The current implementation is a single lazy plan with the
decimal cast inside it (no `/tmp` round-trip), so precision survives
end-to-end. Kept in the trap list as a struck entry because the older
mistake is easy to re-introduce: any pattern that materialises Silver to
parquet and re-reads it for Pandera validation re-trips this. If you
ever bring back a temp parquet, defer the decimal cast to after the
re-read.

### Trap D — Polars 0.20.31 streaming engine has no NDJSON source

`scan_ndjson(...).sink_parquet(...)` errors with
`"not yet supported in standard engine"` even on the trivial plan. Bronze
works around this with a manual chunked Python line iterator
(`_iter_jsonl_chunks`) feeding `pl.read_ndjson` per chunk into a
`pyarrow.parquet.ParquetWriter`. Gold has no JSONL anywhere — do not reach
for `scan_ndjson + sink_parquet` thinking it will stream.

### Trap E — `merchant_subcategory` and `dq_flag` need explicit `pl.Utf8` casts

Both columns are all-null in Stage 1. Polars infers `pl.Null` dtype for an
all-null column, which fails the schema (expects `Utf8`). Always
`pl.lit(None, dtype=pl.Utf8)` or explicit `.cast(pl.Utf8)` when the column
is being created (e.g. in Bronze ingest where `merchant_subcategory` is
absent from every JSON object). On Silver→Gold reads where the column
already exists as `Utf8`, no extra cast is needed.

### Trap F — Eager Silver→Gold reads OOM on a 2 GB cap

Building `fact_transactions` with `pl.read_delta(silver_path)` on the full
22-column Silver frame (audit hashes are ~60 bytes/row of Utf8) materialises
~700 MB before any join. Sort + join allocates an equal-size temporary,
peaking past the 2 GB cap → exit 137. [provision.py](pipeline/provision.py)
uses `pl.scan_delta(...)` with project-pushdown to the 13 columns the Gold
schema actually needs — that lets polars optimise the sort and joins on a
slim frame and keeps peak resident under 1 GB.

### Trap G — delta-rs rust engine refuses warm-cache overwrite without `overwrite_schema=True`

[io_utils.write_delta](pipeline/io_utils.py) uses `engine="rust"`. Polars→arrow
conversion produces `large_string` and tz-aware `timestamp[us, tz=UTC]`,
but delta-rs persists those as `string` and `timestamp[us]` (per Trap B).
On the next `mode="overwrite"`, the rust engine's pre-write schema check
sees the persisted vs incoming schema differ and refuses with
`Schema of data does not match table schema`. The pyarrow engine silently
coerced; the rust engine doesn't. Pass
`overwrite_schema=(mode == "overwrite")` on every `write_deltalake` call
that uses `engine="rust"` (both `io_utils.write_delta` and
`ingest._ingest_transactions` do this).

### Trap H — Docker-for-Mac bind-mount page cache is charged to the container cgroup

On Linux Docker the host's page cache for bind-mounted volumes is largely
not counted against the container's `memory.current`. On Docker-for-Mac
(osxfs / virtiofs) it is. This inflates the local `docker stats` peak by
several hundred MiB compared to what the Linux scoring host will see. The
pipeline calls `posix_fadvise(POSIX_FADV_DONTNEED)` on the JSONL input
(in [ingest._iter_jsonl_chunks](pipeline/ingest.py)) and on the Bronze
parquet tree at the end of `transform`, plus on Silver transactions at
the end of `provision` (via [io_utils.advise_dontneed](pipeline/io_utils.py)).
On the Linux scoring host these calls are quiet no-ops; on Mac they
recover ~200 MiB of headroom by evicting parquet bind-mount caches that
would otherwise stack across stages.

### Trap I — `Iterator[RecordBatch]` write path preserves tz; single-Table strips it

The streaming `write_delta` in [io_utils.py](pipeline/io_utils.py) feeds
`write_deltalake` an iterator of `RecordBatch` (to avoid the doubled
`frame.to_arrow()` copy that the single-Table path forces — see "Anti-
patterns avoided" below). With that input form, the rust engine does NOT
strip the UTC tz label on write — `fact_transactions.ingestion_timestamp`
ends up as `timestamp[us, tz=UTC]` on disk instead of `timestamp[us]`,
breaking the Phase 5 contract assertion and the implicit Trap B behaviour.
The fix is `io_utils._strip_timestamp_tz` + `_cast_batch_to_schema`:
build a tz-stripped target schema from the first slice and re-cast each
batch to it before yielding. The cast is a metadata-only operation on
arrow timestamp arrays (the int64 microseconds are already epoch-UTC),
not a value rewrite.

---

## Verification

Per-phase verification harnesses live under `scripts/`. They are not part
of the pipeline image's CMD; run them with the container's Python entry
point:

```bash
docker run --rm \
  -v "$PWD/output:/data/output" \
  -v "$PWD/scripts:/app/scripts" \
  --entrypoint python \
  nedbank-de-stage1:dev /app/scripts/_phase5_check.py
```

`_phase5_check.py` covers what `validate.py` does not: field counts,
column ordering (incl. GAP-026), surrogate-key uniqueness/range,
`merchant_subcategory`/`dq_flag` all-null in Stage 1, currency uniformity,
on-disk timestamp type, and `(account_sk, customer_sk)` FK integrity.

---

## Stage 2/3 hooks

Commented in place — those stages enable them without restructuring this
file or the config. Search [config/pipeline_config.yaml](config/pipeline_config.yaml)
for `Stage 2:` / `Stage 3:` markers.

| Hook                          | Stage 1 state                              | Stage 2/3 toggle                     |
|-------------------------------|--------------------------------------------|--------------------------------------|
| `currency.accept` map         | `ZAR`/`R`/`RANDS`/`710` all → `ZAR`        | already populated                    |
| `date_formats.candidates`     | `["%Y-%m-%d"]`                             | uncomment `%d/%m/%Y` and `epoch_s`   |
| `province.aliases`            | `{}`                                        | populate with KZN/WC etc.            |
| `dq.enable`                   | `false`                                    | set `true`; populate `dq_rules.yaml` |
| `streaming.*`                 | block commented                            | uncomment in Stage 3 only            |

---

*See `docs/ADR-001-stack-choice.md` for the architectural rationale.*
