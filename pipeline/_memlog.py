"""Cross-stage heap-release helper.

Polars / pyarrow / deltalake each keep their own allocator caches that
aren't released back to the OS on Python-side `del`. Without an explicit
`malloc_trim`, freed buffers stay in glibc arenas and continue to count
as `anon` in cgroup accounting — squeezing the next stage's working-set
budget under a tight memory cap.

`release_heap` runs Python GC and asks glibc to return freed arenas to
the kernel via `malloc_trim(0)`. Call between stages where the previous
stage's working set is no longer needed by the next.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import gc

_LIBC: ctypes.CDLL | None = None


def _libc() -> ctypes.CDLL | None:
    global _LIBC
    if _LIBC is not None:
        return _LIBC
    try:
        name = ctypes.util.find_library("c") or "libc.so.6"
        _LIBC = ctypes.CDLL(name)
        return _LIBC
    except OSError:
        return None


def release_heap() -> None:
    gc.collect()
    libc = _libc()
    if libc is not None:
        try:
            libc.malloc_trim(0)
        except (OSError, AttributeError):
            pass
