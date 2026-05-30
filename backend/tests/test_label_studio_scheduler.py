import asyncio
import time
import unittest
from pathlib import Path

from backend.label_studio_sync import SyncConfig, SyncError
from backend.label_studio_scheduler import LabelStudioScheduler


def _cfg() -> SyncConfig:
    return SyncConfig(
        label_studio_url="http://label-studio:8080",
        label_studio_api_token="ls-token",
        label_studio_project_id=1,
        backend_base_url="http://backend:8000",
        public_image_base_url="http://backend:8000",
        claimed_by="label-studio-sync",
        claim_ttl_seconds=900,
        batch_size=50,
        state_path=Path("/tmp/label-studio-sync-state.json"),
        timeout_seconds=30,
    )


class LabelStudioSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_scheduler_does_not_start(self) -> None:
        scheduler = LabelStudioScheduler(_cfg(), enabled=False)

        started = await scheduler.start()

        self.assertFalse(started)
        self.assertFalse(scheduler.is_running)

    async def test_run_once_runs_export_then_import(self) -> None:
        calls = []

        def export_func(cfg: SyncConfig) -> dict[str, int]:
            calls.append("export")
            return {"exported": 1}

        def import_func(cfg: SyncConfig) -> dict[str, int]:
            calls.append("import")
            return {"imported": 1}

        scheduler = LabelStudioScheduler(_cfg(), export_func=export_func, import_func=import_func)

        summary = await scheduler.run_once()

        self.assertEqual(calls, ["export", "import"])
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["export"], {"exported": 1})
        self.assertEqual(summary["import"], {"imported": 1})

    async def test_run_once_honors_export_and_import_toggles(self) -> None:
        calls = []
        scheduler = LabelStudioScheduler(
            _cfg(),
            run_export_enabled=False,
            run_import_enabled=True,
            export_func=lambda cfg: calls.append("export") or {},
            import_func=lambda cfg: calls.append("import") or {"imported": 1},
        )

        summary = await scheduler.run_once()

        self.assertEqual(calls, ["import"])
        self.assertNotIn("export", summary)
        self.assertEqual(summary["import"], {"imported": 1})

    async def test_run_once_does_not_overlap(self) -> None:
        def slow_export(cfg: SyncConfig) -> dict[str, int]:
            time.sleep(0.1)
            return {"exported": 1}

        scheduler = LabelStudioScheduler(_cfg(), export_func=slow_export, import_func=lambda cfg: {})

        first = asyncio.create_task(scheduler.run_once())
        await asyncio.sleep(0.01)
        second = await scheduler.run_once()
        first_summary = await first

        self.assertEqual(second, {"status": "skipped", "reason": "already_running"})
        self.assertEqual(first_summary["status"], "ok")

    async def test_run_once_catches_sync_errors(self) -> None:
        def failing_export(cfg: SyncConfig) -> dict[str, int]:
            raise SyncError("label studio unavailable")

        scheduler = LabelStudioScheduler(_cfg(), export_func=failing_export, import_func=lambda cfg: {"imported": 0})

        summary = await scheduler.run_once()

        self.assertEqual(summary["status"], "error")
        self.assertIn("label studio unavailable", summary["errors"][0])
        self.assertEqual(summary["import"], {"imported": 0})


if __name__ == "__main__":
    unittest.main()
