"""Cross-process runtime locks for write-heavy monitor jobs."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import logging
from pathlib import Path
import time
from typing import Iterator

LOGGER = logging.getLogger(__name__)

_LOCK_COUNTS: dict[Path, int] = {}
_LOCK_HANDLES: dict[Path, object] = {}


@contextmanager
def runtime_lock(lock_dir: Path, name: str, *, timeout_seconds: float = 60.0, poll_seconds: float = 0.25) -> Iterator[None]:
    """Acquire a re-entrant advisory file lock for one runtime job name."""
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = (lock_dir / f"{name}.lock").resolve()
    if lock_path in _LOCK_COUNTS:
        _LOCK_COUNTS[lock_path] += 1
        try:
            yield
        finally:
            _LOCK_COUNTS[lock_path] -= 1
            if _LOCK_COUNTS[lock_path] <= 0:
                _LOCK_COUNTS.pop(lock_path, None)
        return

    handle = open(lock_path, "a+", encoding="utf-8")
    deadline = time.monotonic() + max(1.0, timeout_seconds)
    wait_started_at = time.monotonic()
    logged_wait = False
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if not logged_wait:
                LOGGER.info("Waiting on runtime lock", extra={"lock_name": name, "lock_path": str(lock_path), "timeout_seconds": timeout_seconds})
                logged_wait = True
            if time.monotonic() >= deadline:
                handle.close()
                raise TimeoutError(f"Timed out waiting for runtime lock '{name}'.")
            time.sleep(max(0.05, poll_seconds))

    _LOCK_COUNTS[lock_path] = 1
    _LOCK_HANDLES[lock_path] = handle
    LOGGER.info(
        "Acquired runtime lock",
        extra={"lock_name": name, "lock_path": str(lock_path), "wait_ms": round((time.monotonic() - wait_started_at) * 1000, 2)},
    )
    try:
        yield
    finally:
        count = _LOCK_COUNTS.get(lock_path, 0) - 1
        if count > 0:
            _LOCK_COUNTS[lock_path] = count
        else:
            _LOCK_COUNTS.pop(lock_path, None)
            stored = _LOCK_HANDLES.pop(lock_path, handle)
            try:
                fcntl.flock(stored.fileno(), fcntl.LOCK_UN)
            finally:
                stored.close()
            LOGGER.info("Released runtime lock", extra={"lock_name": name, "lock_path": str(lock_path)})
