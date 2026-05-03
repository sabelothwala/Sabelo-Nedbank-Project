FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# Additional Python dependencies layered on top of the base image.
# The base image already pins pyspark, delta-spark, pandas, pyarrow, pyyaml,
# duckdb. We add Polars (lazy/streaming engine), deltalake (Rust-backed Delta
# writer — no JVM required), and pandera (schema contracts at layer boundaries).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pipeline source. Config is NOT copied — it is mounted at runtime from
# /data/config/pipeline_config.yaml by the scoring system. Baking config into
# the image would prevent the scorer from injecting overrides.
COPY pipeline/ pipeline/

# Default config path used when PIPELINE_CONFIG is not set. The container
# image keeps a fallback copy under /app/config/ for local-only smoke tests
# (docker run without a /data/config mount). The scorer always mounts its own.
COPY config/ config/

# /app is the WORKDIR but `python pipeline/run_all.py` only puts /app/pipeline
# on sys.path (the script's directory). PYTHONPATH=/app makes
# `from pipeline.ingest import ...` resolve. Without this the orchestrator
# fails with ModuleNotFoundError on the first import.
ENV PYTHONPATH=/app
ENV PIPELINE_CONFIG=/data/config/pipeline_config.yaml

# Polars Decimal is gated behind a feature flag in 0.20.x — without this env
# var, every `cast(pl.Decimal(...))` is a silent no-op and the column stays
# Float64. The Silver and Gold schemas require Decimal(18,2) for amount,
# credit_limit, current_balance; without activation Pandera 0.20.4 has a buggy
# false-pass on Float64 → Decimal which masks the bug, but the Delta tables
# end up off-contract.
ENV POLARS_ACTIVATE_DECIMAL=1

# The scoring system invokes:
#   docker run [security flags] <image> python pipeline/run_all.py
# Do not change this CMD without updating docker_interface_contract.md alignment.
CMD ["python", "pipeline/run_all.py"]
