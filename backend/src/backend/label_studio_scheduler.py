from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Callable
from typing import Any

from psycopg.errors import Error as PsycopgError

from backend.label_studio_sync import SyncConfig, SyncError, run_export, run_import
from backend.services import logger


SyncFunc = Callable[[SyncConfig], dict[str, Any]]


class LabelStudioScheduler:
    def __init__(
        self,
        cfg: SyncConfig,
        *,
        enabled: bool = True,
        interval_seconds: int = 300,
        run_on_startup: bool = True,
        run_export_enabled: bool = True,
        run_import_enabled: bool = True,
        export_func: SyncFunc = run_export,
        import_func: SyncFunc = run_import,
        sleep_func: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.cfg = cfg
        self.enabled = enabled
        self.interval_seconds = max(1, interval_seconds)
        self.run_on_startup = run_on_startup
        self.run_export_enabled = run_export_enabled
        self.run_import_enabled = run_import_enabled
        self.export_func = export_func
        self.import_func = import_func
        self.sleep_func = sleep_func
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> bool:
        if not self.enabled:
            logger.info("label studio scheduler disabled")
            return False
        if self.is_running:
            return True
        self._task = asyncio.create_task(self._run_loop(), name="label-studio-sync-scheduler")
        logger.info("label studio scheduler started")
        return True

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        logger.info("label studio scheduler stopped")

    async def run_once(self) -> dict[str, Any]:
        if self._lock.locked():
            return {"status": "skipped", "reason": "already_running"}

        async with self._lock:
            started = time.perf_counter()
            summary: dict[str, Any] = {"status": "ok", "errors": []}

            if self.run_export_enabled:
                try:
                    summary["export"] = await asyncio.to_thread(self.export_func, self.cfg)
                except (SyncError, PsycopgError, Exception) as exc:
                    summary["status"] = "error"
                    summary["errors"].append(f"export failed: {exc}")

            if self.run_import_enabled:
                try:
                    summary["import"] = await asyncio.to_thread(self.import_func, self.cfg)
                except (SyncError, PsycopgError, Exception) as exc:
                    summary["status"] = "error"
                    summary["errors"].append(f"import failed: {exc}")

            if not summary["errors"]:
                summary.pop("errors")
            summary["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
            logger.info(f"label studio sync cycle complete: {json.dumps(summary, default=str)}")
            return summary

    async def _run_loop(self) -> None:
        if self.run_on_startup:
            await self.run_once()

        while True:
            await self.sleep_func(self.interval_seconds)
            await self.run_once()
