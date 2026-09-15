from __future__ import annotations

"""
Folder watcher — powered by the `watchdog` library (pip install watchdog).

Replaces the old manual poll loop (asyncio.sleep + os.listdir) with OS-native
filesystem events:
  Linux  (EKS pods)  → inotify  — kernel delivers events instantly, zero idle CPU
  macOS  (dev)       → FSEvents — same instant delivery
  Fallback           → PollingObserver — used automatically on network-mounted
                       volumes (Docker PVC mounts on EKS often don't forward
                       inotify to the container). Behaviour is identical to the
                       old loop but managed by the library.

What this module does (same as before):
  1. Detect new .json files in PIPELINE_DEFINITIONS_DIR
  2. Validate against PipelineDefinition schema
  3. Register pipeline in Redis (desired_state=stopped)
  4. Move file → processed/ (success) or failed/ (error)

What it does NOT do (same as before):
  Does NOT start containers.
  Does NOT set desired_state=running.
  Does NOT handle .stop.json or .delete.json files.

Two-replica safety
------------------
Both Task Manager replicas run this watcher on the same mounted directory.
If both see the same file simultaneously:
  - Replica A: create_pipeline() succeeds, shutil.move() succeeds
  - Replica B: create_pipeline() raises ValueError (pipeline exists) → caught →
               file already moved → shutil.move() raises FileNotFoundError → caught
  Net: pipeline registered exactly once, no duplicates, no crashes.

Thread bridge
-------------
watchdog's Observer runs in its own background thread.
_handle() is an async coroutine (needs Redis).
Bridge: asyncio.run_coroutine_threadsafe(coro, loop) submits the coroutine to
the running asyncio event loop and blocks the observer thread until it completes.
All existing async Redis logic is unchanged.
"""

import asyncio
import json
import logging
import shutil
import threading
from pathlib import Path

from pydantic import ValidationError
from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from task_manager.app.config import settings
from task_manager.app.pipeline_definition import PipelineDefinition
from common.app_common.models import PipelineConfig, PipelineType
from common.app_common.redis_repo import PipelineRedisRepository

logger = logging.getLogger("task-manager.watcher")

_PROCESSED = "processed"
_FAILED    = "failed"


# ══════════════════════════════════════════════════════════════════════════
# Observer event handler — runs in watchdog's background thread
# ══════════════════════════════════════════════════════════════════════════

class _DefinitionFileHandler(FileSystemEventHandler):
    """
    Receives CREATE filesystem events from the watchdog Observer.
    Bridges synchronous Observer callbacks to the asyncio event loop.
    """

    def __init__(
        self,
        repo: PipelineRedisRepository,
        loop: asyncio.AbstractEventLoop,
        watch_path: Path,
    ) -> None:
        super().__init__()
        self._repo       = repo
        self._loop       = loop
        self._watch_path = watch_path
        # Prevent double-processing the same file if the OS fires multiple events
        self._in_flight: set[str] = set()
        self._lock = threading.Lock()

    def on_created(self, event: FileCreatedEvent) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix != ".json":
            return
        # Ignore files in processed/ or failed/ subdirectories
        if path.parent != self._watch_path:
            return

        with self._lock:
            if path.name in self._in_flight:
                return
            self._in_flight.add(path.name)

        logger.info("Watcher: new file detected — %s", path.name)

        # Submit the async handler to the event loop.
        # This blocks the observer thread until _handle() completes.
        future = asyncio.run_coroutine_threadsafe(
            self._process(path), self._loop
        )
        try:
            future.result(timeout=30)
        except Exception:
            logger.exception("Watcher: error processing %s", path.name)
            _move(path, self._watch_path / _FAILED)

    async def _process(self, path: Path) -> None:
        try:
            await _handle(path, self._repo)
        finally:
            with self._lock:
                self._in_flight.discard(path.name)


# ══════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════

async def folder_watcher_loop(repo: PipelineRedisRepository) -> None:
    """
    Long-running background coroutine. Wire into FastAPI lifespan via
    asyncio.create_task().

    Starts watchdog Observer in a background thread.
    Automatically falls back to PollingObserver on network-mounted volumes.
    Processes any files that arrived while the service was offline on startup.
    Restarts the Observer thread if it dies unexpectedly.
    """
    watch_path = Path(settings.pipeline_definitions_dir)
    watch_path.mkdir(parents=True, exist_ok=True)
    (watch_path / _PROCESSED).mkdir(exist_ok=True)
    (watch_path / _FAILED).mkdir(exist_ok=True)

    loop    = asyncio.get_running_loop()
    handler = _DefinitionFileHandler(repo, loop, watch_path)

    observer = _start_observer(handler, watch_path)
    logger.info(
        "Folder watcher started — dir=%s observer=%s",
        watch_path,
        type(observer).__name__,
    )

    # Catch files that arrived while we were down
    await _scan_existing(watch_path, repo)

    try:
        while True:
            await asyncio.sleep(1)
            if not observer.is_alive():
                logger.warning("Watcher: observer thread died — restarting")
                observer.stop()
                observer = _start_observer(handler, watch_path)
                logger.info("Watcher: observer restarted as %s", type(observer).__name__)
    except asyncio.CancelledError:
        logger.info("Folder watcher stopping...")
    finally:
        observer.stop()
        observer.join()
        logger.info("Folder watcher stopped")


# ══════════════════════════════════════════════════════════════════════════
# File processing
# ══════════════════════════════════════════════════════════════════════════

async def _handle(fp: Path, repo: PipelineRedisRepository) -> None:
    """Validate one definition file, register in Redis, move to processed/ or failed/."""
    if not fp.exists():
        # Already moved by the other replica
        logger.debug("Watcher: %s already gone (processed by other replica)", fp.name)
        return

    # Parse JSON
    try:
        raw = json.loads(fp.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Watcher: cannot read %s — %s", fp.name, exc)
        _move(fp, fp.parent / _FAILED)
        return

    # Validate schema
    try:
        defn = PipelineDefinition.model_validate(raw)
    except ValidationError as exc:
        logger.error("Watcher: schema error in %s\n%s", fp.name, exc)
        _move(fp, fp.parent / _FAILED)
        return

    pid   = defn.pipeline_id
    topic = defn.topic or f"pipeline.{pid}.events"

    # Already registered?
    if await repo.get_config(pid) is not None:
        logger.info("Watcher: '%s' already registered — moved to processed", pid)
        _move(fp, fp.parent / _PROCESSED)
        return

    # Register in Redis
    config = PipelineConfig(
        pipeline_id=pid,
        pipeline_type=PipelineType.LIVE,
        source_type=defn.source_type,
        topic=topic,
        batch_size=defn.batch_size,
        flush_interval_seconds=defn.flush_interval_seconds,
        source_options=defn.source_options,
    )
    try:
        await repo.create_pipeline(config)
        logger.info(
            "Watcher: '%s' registered (source=%s) — "
            "start via POST /pipelines/%s/start",
            pid, defn.source_type, pid,
        )
        _move(fp, fp.parent / _PROCESSED)
    except ValueError:
        # Race: other replica registered between our get_config and create_pipeline
        logger.info("Watcher: '%s' registered by other replica — moved to processed", pid)
        _move(fp, fp.parent / _PROCESSED)
    except Exception:
        logger.exception("Watcher: failed to register '%s'", pid)
        _move(fp, fp.parent / _FAILED)


async def _scan_existing(watch_path: Path, repo: PipelineRedisRepository) -> None:
    """Process files that arrived while the service was offline."""
    pending = sorted(
        f for f in watch_path.iterdir()
        if f.is_file() and f.suffix == ".json"
    )
    if not pending:
        return
    logger.info("Watcher: %d file(s) found on startup — processing", len(pending))
    for fp in pending:
        try:
            await _handle(fp, repo)
        except Exception:
            logger.exception("Watcher: error handling startup file %s", fp.name)


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _start_observer(handler: _DefinitionFileHandler, watch_path: Path) -> Observer:
    """
    Try native Observer first (inotify on Linux, FSEvents on macOS).
    Fall back to PollingObserver for network mounts that don't support inotify.
    """
    for cls in (Observer, PollingObserver):
        try:
            obs = cls()
            if cls is PollingObserver:
                # Use the configured poll interval for the fallback observer
                obs = cls(timeout=settings.watcher_poll_interval_s)
            obs.schedule(handler, str(watch_path), recursive=False)
            obs.start()
            return obs
        except Exception as exc:
            logger.warning("Watcher: %s failed (%s) — trying fallback", cls.__name__, exc)
    raise RuntimeError("Could not start any filesystem observer — check permissions on watch dir")


def _move(fp: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / fp.name
    if target.exists():
        n = 1
        while (dest / f"{fp.stem}.{n}{fp.suffix}").exists():
            n += 1
        target = dest / f"{fp.stem}.{n}{fp.suffix}"
    try:
        shutil.move(str(fp), str(target))
    except (OSError, FileNotFoundError) as exc:
        # FileNotFoundError = other replica already moved it — not an error
        logger.debug("Watcher: could not move %s — %s", fp.name, exc)
