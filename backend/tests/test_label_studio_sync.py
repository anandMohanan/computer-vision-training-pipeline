import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from urllib.error import HTTPError

from backend.label_studio_sync import (
    LabelStudioClient,
    LocalJsonStateStore,
    SyncConfig,
    SyncError,
    _annotation_key,
    _http_json,
    _status_from_ls_annotation,
    _to_backend_annotation,
    _to_task_payload,
    run_export,
    run_import,
)


def _cfg(state_path: Path) -> SyncConfig:
    return SyncConfig(
        label_studio_url="http://label-studio:8080",
        label_studio_api_token="ls-token",
        label_studio_project_id=123,
        backend_base_url="http://backend:8000",
        public_image_base_url="http://public-backend:8000",
        claimed_by="label-studio-sync",
        claim_ttl_seconds=900,
        batch_size=50,
        state_path=state_path,
        timeout_seconds=7,
    )


class LabelStudioSyncTests(unittest.TestCase):
    def test_status_mapping_reviewed_default(self) -> None:
        self.assertEqual(_status_from_ls_annotation({}), "reviewed")

    def test_status_mapping_verified_ground_truth(self) -> None:
        self.assertEqual(_status_from_ls_annotation({"ground_truth": True}), "verified")

    def test_status_mapping_rejected_cancelled(self) -> None:
        self.assertEqual(_status_from_ls_annotation({"was_cancelled": True}), "rejected")

    def test_backend_annotation_payload_mapping(self) -> None:
        task = {
            "id": 17,
            "data": {"sample_id": "sample-001"},
            "updated_at": "2026-05-30T10:00:00Z",
        }
        annotation = {
            "id": 99,
            "completed_by": {"email": "annotator@example.com"},
            "result": [{"id": "r1"}],
            "updated_at": "2026-05-30T10:30:00Z",
        }

        payload = _to_backend_annotation(task, annotation)

        self.assertEqual(payload["sample_id"], "sample-001")
        self.assertEqual(payload["tool_name"], "label-studio")
        self.assertEqual(payload["annotator_id"], "annotator@example.com")
        self.assertEqual(payload["status"], "reviewed")
        self.assertEqual(payload["labels"], {"raw_result": [{"id": "r1"}]})
        self.assertEqual(payload["reviewed_at"], "2026-05-30T10:30:00Z")

    def test_backend_annotation_payload_prefers_completed_at(self) -> None:
        payload = _to_backend_annotation(
            {"id": 17, "data": {"sample_id": "sample-001"}, "updated_at": "2026-05-30T10:00:00Z"},
            {
                "id": 99,
                "completed_by": {"username": "reviewer"},
                "result": [],
                "completed_at": "2026-05-30T10:20:00Z",
                "updated_at": "2026-05-30T10:30:00Z",
            },
        )

        self.assertEqual(payload["annotator_id"], "reviewer")
        self.assertEqual(payload["reviewed_at"], "2026-05-30T10:20:00Z")

    def test_backend_annotation_payload_scalar_completed_by(self) -> None:
        payload = _to_backend_annotation(
            {"id": 17, "data": {"sample_id": "sample-001"}, "updated_at": "2026-05-30T10:00:00Z"},
            {"id": 99, "completed_by": 42, "result": []},
        )

        self.assertEqual(payload["annotator_id"], "42")

    def test_task_payload_uses_public_image_url_and_project(self) -> None:
        with TemporaryDirectory() as tmp:
            cfg = _cfg(Path(tmp) / "state.json")
            payload = _to_task_payload(
                {
                    "sample_id": "sample-001",
                    "object_url": "/samples/2026/05/30/device/sample-001.jpg",
                    "device_id": "edge-cam-01",
                    "captured_at": "2026-05-30T10:00:00+00:00",
                    "uncertainty_score": 0.91,
                },
                cfg,
            )

        self.assertEqual(payload["project"], 123)
        self.assertIs(payload["allow_skip"], True)
        self.assertEqual(payload["data"]["sample_id"], "sample-001")
        self.assertEqual(payload["data"]["image"], "http://public-backend:8000/samples/2026/05/30/device/sample-001.jpg")

    def test_label_studio_client_uses_token_auth(self) -> None:
        with TemporaryDirectory() as tmp:
            client = LabelStudioClient(_cfg(Path(tmp) / "state.json"))

        self.assertEqual(client.headers, {"Authorization": "Token ls-token"})

    def test_http_json_includes_error_body(self) -> None:
        def failing_urlopen(request, timeout):  # noqa: ANN001
            raise HTTPError(request.full_url, 500, "server error", hdrs=None, fp=Mock(read=lambda: b"broken"))

        with self.assertRaisesRegex(SyncError, "HTTP 500 GET http://service.test/api: broken"):
            _http_json("GET", "http://service.test/api", timeout_seconds=3, opener=failing_urlopen)

    def test_label_studio_task_pagination_accepts_tasks_and_results(self) -> None:
        with TemporaryDirectory() as tmp:
            cfg = _cfg(Path(tmp) / "state.json")
            calls = []

            def fake_request(method, path, *, params=None, payload=None):  # noqa: ANN001
                calls.append((method, path, params, payload))
                if params["page"] == 1:
                    return {"tasks": [{"id": 1, "annotations": []}], "total": 2}
                if params["page"] == 2:
                    return {"results": [{"id": 2, "annotations": []}], "total": 2}
                return {"tasks": [], "total": 2}

            client = LabelStudioClient(cfg)
            client.request = fake_request

            tasks = list(client.iter_annotated_tasks())

        self.assertEqual([task["id"] for task in tasks], [1, 2])
        self.assertEqual(calls[0][2]["only_annotated"], "true")
        self.assertEqual(calls[0][2]["fields"], "all")

    def test_run_export_skips_existing_samples(self) -> None:
        with TemporaryDirectory() as tmp:
            cfg = _cfg(Path(tmp) / "state.json")
            state_store = LocalJsonStateStore(cfg.state_path)
            state_store.record_exported_task("existing", cfg.label_studio_project_id, 77)
            backend = Mock()
            backend.get_review_candidates.return_value = [
                {"sample_id": "existing", "object_url": "/samples/existing.jpg"},
                {
                    "sample_id": "new",
                    "object_url": "/samples/new.jpg",
                    "device_id": "cam-01",
                    "captured_at": "2026-05-30T10:00:00+00:00",
                    "uncertainty_score": 0.5,
                },
            ]
            label_studio = Mock()
            label_studio.create_task.return_value = {"id": 88}

            summary = run_export(cfg, backend_client=backend, label_studio_client=label_studio, state_store=state_store)

        self.assertEqual(summary, {"fetched": 2, "claimed": 1, "exported": 1, "skipped_existing": 1, "failed": 0})
        backend.claim_sample.assert_called_once_with("new")
        label_studio.create_task.assert_called_once()

    def test_run_import_skips_imported_annotation_keys(self) -> None:
        with TemporaryDirectory() as tmp:
            cfg = _cfg(Path(tmp) / "state.json")
            state_store = LocalJsonStateStore(cfg.state_path)
            key = _annotation_key(17, {"id": 99})
            state_store.add_imported_annotation_key("sample-001", cfg.label_studio_project_id, key)
            label_studio = Mock()
            label_studio.iter_annotated_tasks.return_value = [
                {
                    "id": 17,
                    "data": {"sample_id": "sample-001"},
                    "annotations": [
                        {"id": 99, "completed_by": "a", "result": []},
                        {"id": 100, "completed_by": "b", "result": []},
                    ],
                }
            ]
            backend = Mock()

            summary = run_import(cfg, backend_client=backend, label_studio_client=label_studio, state_store=state_store)

        self.assertEqual(summary, {"tasks": 1, "annotations_inspected": 2, "imported": 1, "skipped_duplicate": 1, "failed": 0})
        backend.write_annotation.assert_called_once()


if __name__ == "__main__":
    unittest.main()
