"""Pipeline orchestrator.

Invoked by the Docker CMD as:
  python pipeline/run_all.py

Chains the three medallion stages and the validation harness. Any stage
raising propagates to a non-zero exit — the scoring system reads exit code
before reading outputs, so half-written Delta tables never reach scoring.

No CLI args, no stdin reads. The container has no TTY.
"""

from __future__ import annotations

import logging
import sys
import time

from pipeline._memlog import release_heap
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.transform import run_transformation
from pipeline.validate import run_validation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("pipeline.run_all")


def main() -> int:
    t0 = time.monotonic()
    try:
        log.info("stage=ingest start")
        run_ingestion()
        log.info("stage=ingest done elapsed=%.1fs", time.monotonic() - t0)
        release_heap()

        t1 = time.monotonic()
        log.info("stage=transform start")
        run_transformation()
        log.info("stage=transform done elapsed=%.1fs", time.monotonic() - t1)
        release_heap()

        t2 = time.monotonic()
        log.info("stage=provision start")
        run_provisioning()
        log.info("stage=provision done elapsed=%.1fs", time.monotonic() - t2)
        release_heap()

        t3 = time.monotonic()
        log.info("stage=validate start")
        run_validation()
        log.info("stage=validate done elapsed=%.1fs", time.monotonic() - t3)
    except Exception:
        log.exception("pipeline failed")
        return 1
    log.info("pipeline ok total_elapsed=%.1fs", time.monotonic() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
