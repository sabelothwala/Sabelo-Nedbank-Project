# Nedbank N*ovation Data Engineering Challenge — Stage 1

**Nedbank N*ovation 2026 · Data Engineering Track** — a competitive individual challenge for South African data engineers, evaluated on correctness, scalability, maintainability, and efficiency against anonymised real banking data.

The challenge: ingest three daily batch feeds from a bank-fintech partnership (accounts, transactions, customers) and build a clean, auditable, analytics-ready medallion pipeline — inside a 2 GB / 2 vCPU Docker container with no internet access, in under 15 minutes of wall time.

Completed Stage 1 (the core batch pipeline). Stages 2 and 3 (stress-test with injected DQ failures and a streaming path) are designed in the config hooks but not yet implemented — see the Stage 2/3 hooks table below.

---

## What the pipeline does

Three source feeds in, one queryable dimensional model out:

| Source | Format | Volume |
|---|---|---|
| `accounts.csv` | CSV | ~100k rows |
| `transactions.jsonl` | JSONL | ~1M rows |
| `customers.csv` | CSV | ~80k rows |

**Bronze** — raw ingest, unmodified, with an `ingestion_timestamp`, partitioned by source.  
**Silver** — typed, standardised dates, deduplicated on PKs, account-to-customer linkage resolved, schema enforced via Pandera contracts.  
**Gold** — a star-schema dimensional model: `fact_transactions` (15 fields) joined to `dim_accounts` (11 fields) and `dim_customers` (9 fields, including a derived `age_band`). Passes all three scored validation queries.

Runs end-to-end in **~15 s** on a 2 GB / 2 vCPU container with `--read-only --network=none`.

---

## Stack

**Polars (lazy/streaming)** + **`deltalake` (Rust)** + **Pandera** + **DuckDB**.

PySpark is in the base image and untouched — see `docs/ADR-001-stack-choice.md` for why. Short version: PySpark's JVM startup alone costs more than the entire Polars pipeline on Stage 1 data volumes, and the Polars lazy + project-pushdown plan keeps peak memory under 1 GB inside the 2 GB cap.

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

Exit 0 with the three validation query results on stderr indicates a clean Stage 1 submission. To watch peak memory, run `docker stats --no-stream de_e2e` in a second terminal while the pipeline is alive.

---

## POPIA §19 audit trail

POPIA §19 requires that personal-data processing be auditable: who processed the data, when, in which run, and over which source bytes. Every Bronze write appends four columns; they are preserved through Silver, then dropped in Gold (except `_ingested_at`, which is renamed to `fact_transactions.ingestion_timestamp`).

| Column | Type | Purpose |
|---|---|---|
| `_ingested_at` | `Datetime(us,UTC)` | wall-clock UTC timestamp when the row entered Bronze |
| `_source_system` | `Utf8` | logical source name from `cfg.sources.<src>.source_system` |
| `_batch_id` | `Utf8` | per-run id `yyyyMMddTHHMMSSZ-<6hex>`, sortable by run |
| `_source_row_hash` | `Utf8` | xxhash64 over the source columns only (audit cols excluded) |

xxhash64 rather than sha256: drift detection is the use case, not cryptographic resistance. A pure-Python sha256 over the 1M-row transactions frame costs ~30 s — over twice the entire pipeline runtime. Collision probability on 1M rows at 64 bits is ≈ 5 × 10⁻⁸, which is acceptable for this purpose.

---

## Three-tier schema drift policy

Configured via `cfg.drift.policy`. Stage 1 ships with `strict` at the Pandera boundary; the `quarantine` sink (the POPIA-aligned production default) is the Stage 2 build-out.

| Tier | Behaviour | Wired in Stage 1? |
|---|---|---|
| `strict` | Pipeline exits non-zero on any unknown column | Yes (Bronze + Silver) |
| `quarantine` | Unknown column diverted to `silver/_quarantine/<src>/`; in-band row kept minus the unknown column; WARN logged | Stage 2 build-out |
| `autoevolve` | Unknown columns silently extend the Silver schema | No, intentionally |

**Why quarantine is the POPIA-aligned default:** POPIA §13(2) requires processing be necessary for an explicitly defined purpose. A new source column is, by definition, not yet covered by a defined purpose. Quarantine threads the needle: in-band rows are processed minus the unknown column, the column lands in a side path for a human to map, and a WARN tells the data team something needs a definition.

---

## Anti-patterns avoided

Decisions that would have looked fine on Stage 1 data and broken at Stage 2 scale:

- **No `.collect()` on the 1M-row transactions frame in lazy chains.** `provision._provision_fact_transactions` builds a single lazy plan (scan → sort → row_index → join × 2 → select) and `.collect()`s exactly once. The eager version OOM-killed at exit 137 inside the 2 GB cap; lazy + project-pushdown kept peak resident under 1 GB.
- **No pandas at scale.** Every transformation stays in Polars lazy plans or Arrow record batches. No `.to_pandas()` calls in the hot path.
- **No hardcoded paths or thresholds.** All source paths, schema field lists, and config values are externalised to `pipeline_config.yaml` — Stage 2's curveballs are additive config changes, not rewrites.
- **No redundant Delta scans.** `provision.py` uses `scan_delta` with project-pushdown to the 13 columns the Gold schema needs, not a full `read_delta` of the 22-column Silver frame.

---

## Traps surfaced during development

Eight non-obvious issues diagnosed during Phases 3-7. Documented to save the next author ~8 hours.

**Trap A — Polars 0.20.x Decimal is gated behind a feature flag.** Without `ENV POLARS_ACTIVATE_DECIMAL=1` in the Dockerfile, every `cast(pl.Decimal(...))` is a silent no-op. Worse, pandera 0.20.4 has a buggy false-pass on Float64-vs-Decimal, masking the bug entirely.

**Trap B — `deltalake` round-trip drops UTC tz labels.** `_ingested_at` is written as `Datetime("us", "UTC")` but reads back as `Datetime("us", None)`. Every Bronze→Silver and Silver→Gold read re-tags with `.dt.replace_time_zone("UTC")`.

**Trap D — Polars 0.20.31 streaming engine has no NDJSON source.** `scan_ndjson(...).sink_parquet(...)` errors even on a trivial plan. Bronze works around this with a manual chunked Python line iterator feeding `pl.read_ndjson` per chunk.

**Trap F — Eager Silver→Gold reads OOM on a 2 GB cap.** `pl.read_delta` on the full 22-column Silver frame materialises ~700 MB before any join. Sort + join allocation pushes past 2 GB → exit 137. `scan_delta` with project-pushdown keeps peak under 1 GB.

**Trap G — delta-rs rust engine refuses warm-cache overwrite without `overwrite_schema=True`.** Polars→Arrow conversion produces `large_string` and tz-aware timestamps; delta-rs persists them differently. On the next `mode="overwrite"` the schema check fails. Pass `overwrite_schema=(mode == "overwrite")` on every `write_deltalake` call.

**Trap H — Docker-for-Mac bind-mount page cache is charged to the container cgroup.** On Linux Docker it largely isn't. The pipeline calls `posix_fadvise(POSIX_FADV_DONTNEED)` on key paths; on Mac this recovers ~200 MiB of headroom, on Linux it's a quiet no-op.

Full trap documentation (including Traps C, E, I) is in the inline comments of the relevant source files.

---

## Stage 2/3 hooks

Commented in place — those stages enable them without restructuring this file or the config. Search `config/pipeline_config.yaml` for `Stage 2:` / `Stage 3:` markers.

| Hook | Stage 1 state | Stage 2/3 toggle |
|---|---|---|
| `currency.accept` map | `ZAR`/`R`/`RANDS`/`710` all → `ZAR` | already populated |
| `date_formats.candidates` | `["%Y-%m-%d"]` | uncomment `%d/%m/%Y` and `epoch_s` |
| `province.aliases` | `{}` | populate with KZN/WC etc. |
| `dq.enable` | `false` | set `true`; populate `dq_rules.yaml` |
| `streaming.*` | block commented | uncomment in Stage 3 only |

---

*See `docs/ADR-001-stack-choice.md` for the full architectural rationale.*
