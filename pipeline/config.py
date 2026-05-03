"""Configuration loader.

Reads pipeline_config.yaml from the path in the PIPELINE_CONFIG env var,
falling back to /data/config/pipeline_config.yaml (the scorer's mount point),
then to the in-image copy at /app/config/pipeline_config.yaml for local smoke
tests. Returns a plain dict — no validation here; Pandera validates the
*data*, not the config shape, and the call sites raise KeyError loudly if a
required key is missing.

Why no Pydantic / dataclass wrapper: every config-shape change would require a
code change in two places. A plain dict + KeyError on missing keys keeps the
config the single source of truth.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_PATHS = (
    "/data/config/pipeline_config.yaml",
    "/app/config/pipeline_config.yaml",
)


def _resolve_path() -> Path:
    env_path = os.environ.get("PIPELINE_CONFIG")
    candidates = [env_path, *_DEFAULT_PATHS] if env_path else list(_DEFAULT_PATHS)
    for p in candidates:
        if p and Path(p).is_file():
            return Path(p)
    raise FileNotFoundError(
        f"pipeline_config.yaml not found. Tried: {candidates}. "
        "Set PIPELINE_CONFIG or mount the config at /data/config/."
    )


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    path = _resolve_path()
    with path.open() as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config at {path} must be a YAML mapping, got {type(cfg)}")
    cfg["_resolved_config_path"] = str(path)
    return cfg
