from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from psycopg import connect
from psycopg.errors import Error as PsycopgError
from psycopg.rows import dict_row
from psycopg.types.json import Json


@dataclass(frozen=True)
class SyncConfig:
    label_studio_url: str
    label_studio_api_token: str
    label_studio_project_id: int
    backend_base_url: str
    public_image_base_url: str
    claimed_by: str
    claim_ttl_seconds: int
    batch_size: int
    state_path: Path
    timeout_seconds: int


class SyncError(RuntimeError):
    pass


def _must_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SyncError(f"missing required environment variable: {name}")
    return value


def _load_config() -> SyncConfig:
    root_dir = Path(__file__).resolve().parents[3]
    state_path = Path(os.getenv("LABEL_STUDIO_SYNC_STATE_PATH", str(root_dir / "backend" / ".label_studio_sync_state.json")))
    return SyncConfig(
        label_studio_url=_must_env("LABEL_STUDIO_URL").rstrip("/"),
        label_studio_api_token=_must_env("LABEL_STUDIO_API_TOKEN"),
        label_studio_project_id=int(_must_env("LABEL_STUDIO_PROJECT_ID")),
        backend_base_url=os.getenv("BACKEND_BASE_URL", "http://localhost:8000").rstrip("/"),
        public_image_base_url=os.getenv("PUBLIC_IMAGE_BASE_URL", os.getenv("BACKEND_BASE_URL", "http://localhost:8000")).rstrip("/"),
        claimed_by=os.getenv("LABEL_STUDIO_CLAIMED_BY", "label-studio-sync"),
        claim_ttl_seconds=int(os.getenv("LABEL_STUDIO_CLAIM_TTL_SECONDS", "900")),
        batch_size=int(os.getenv("LABEL_STUDIO_SYNC_BATCH_SIZE", "50")),
        state_path=state_path,
        timeout_seconds=int(os.getenv("LABEL_STUDIO_SYNC_TIMEOUT_SECONDS", "30")),
    )


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"sample_to_task": {}, "imported_annotation_keys": []}
    with path.open("r", encoding="utf-8") as f:
        state = json.load(f)
    state.setdefault("sample_to_task", {})
    state.setdefault("imported_annotation_keys", [])
    return state


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def _http_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    payload: Any | None = None,
    timeout_seconds: int = 30,
    opener: Callable[..., Any] = urlopen,
) -> Any:
    data = None
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = Request(url=url, method=method, headers=req_headers, data=data)
    try:
        with opener(req, timeout=timeout_seconds) as res:
            body = res.read().decode("utf-8")
            if not body:
                return {}
            return json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise SyncError(f"HTTP {exc.code} {method} {url}: {body}") from exc
    except URLError as exc:
        raise SyncError(f"network error {method} {url}: {exc}") from exc


class BackendClient:
    def __init__(self, cfg: SyncConfig) -> None:
        self.cfg = cfg

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, payload: Any | None = None) -> Any:
        query = f"?{urlencode(params)}" if params else ""
        return _http_json(
            method,
            f"{self.cfg.backend_base_url}{path}{query}",
            payload=payload,
            timeout_seconds=self.cfg.timeout_seconds,
        )

    def get_review_candidates(self) -> list[dict[str, Any]]:
        res = self.request(
            "GET",
            "/v1/queue/review-candidates",
            params={"limit": self.cfg.batch_size, "per_device_cap": self.cfg.batch_size},
        )
        return res.get("candidates", [])

    def claim_sample(self, sample_id: str) -> None:
        self.request(
            "POST",
            "/v1/queue/claim",
            payload={
                "sample_id": sample_id,
                "claimed_by": self.cfg.claimed_by,
                "ttl_seconds": self.cfg.claim_ttl_seconds,
            },
        )

    def write_annotation(self, payload: dict[str, Any]) -> None:
        self.request("POST", "/v1/annotations", payload=payload)


class LabelStudioClient:
    def __init__(self, cfg: SyncConfig) -> None:
        self.cfg = cfg
        self.headers = {"Authorization": f"Token {cfg.label_studio_api_token}"}

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, payload: Any | None = None) -> Any:
        query = f"?{urlencode(params)}" if params else ""
        return _http_json(
            method,
            f"{self.cfg.label_studio_url}{path}{query}",
            headers=self.headers,
            payload=payload,
            timeout_seconds=self.cfg.timeout_seconds,
        )

    def create_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        res = self.request("POST", "/api/tasks/", payload=payload)
        if not isinstance(res, dict) or "id" not in res:
            raise SyncError(f"label studio task create response missing id: {res}")
        return res

    def iter_annotated_tasks(self):
        page = 1
        yielded = 0
        while True:
            res = self.request(
                "GET",
                "/api/tasks",
                params={
                    "project": self.cfg.label_studio_project_id,
                    "only_annotated": "true",
                    "fields": "all",
                    "page_size": self.cfg.batch_size,
                    "page": page,
                },
            )
            tasks = res if isinstance(res, list) else res.get("tasks", res.get("results", []))
            if not tasks:
                break
            for task in tasks:
                yielded += 1
                yield task

            total = None if isinstance(res, list) else res.get("total") or res.get("count")
            if total is not None:
                if yielded >= int(total):
                    break
            elif len(tasks) < self.cfg.batch_size:
                break
            page += 1


class SyncStateStore(Protocol):
    def exported_sample_ids(self, project_id: int, sample_ids: list[str]) -> set[str]:
        ...

    def record_exported_task(self, sample_id: str, project_id: int, task_id: int) -> None:
        ...

    def imported_annotation_keys(self, sample_id: str, project_id: int) -> set[str]:
        ...

    def add_imported_annotation_key(self, sample_id: str, project_id: int, key: str) -> None:
        ...


class LocalJsonStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _load(self) -> dict[str, Any]:
        return _load_state(self.path)

    def _save(self, state: dict[str, Any]) -> None:
        _save_state(self.path, state)

    def exported_sample_ids(self, project_id: int, sample_ids: list[str]) -> set[str]:
        state = self._load()
        sample_to_task = state.setdefault("sample_to_task", {})
        return {sample_id for sample_id in sample_ids if sample_id in sample_to_task}

    def record_exported_task(self, sample_id: str, project_id: int, task_id: int) -> None:
        state = self._load()
        state.setdefault("sample_to_task", {})[sample_id] = task_id
        tasks = state.setdefault("label_studio_tasks", {})
        tasks[sample_id] = {"project_id": project_id, "task_id": task_id}
        self._save(state)

    def imported_annotation_keys(self, sample_id: str, project_id: int) -> set[str]:
        state = self._load()
        by_sample = state.setdefault("imported_annotation_keys_by_sample", {})
        keys = set(by_sample.get(sample_id, []))
        keys.update(state.setdefault("imported_annotation_keys", []))
        return keys

    def add_imported_annotation_key(self, sample_id: str, project_id: int, key: str) -> None:
        state = self._load()
        by_sample = state.setdefault("imported_annotation_keys_by_sample", {})
        sample_keys = set(by_sample.get(sample_id, []))
        sample_keys.add(key)
        by_sample[sample_id] = sorted(sample_keys)
        global_keys = set(state.setdefault("imported_annotation_keys", []))
        global_keys.add(key)
        state["imported_annotation_keys"] = sorted(global_keys)
        self._save(state)


class PostgresSyncStateStore:
    def __init__(self, pg_dsn: str) -> None:
        self.pg_dsn = pg_dsn

    def exported_sample_ids(self, project_id: int, sample_ids: list[str]) -> set[str]:
        if not sample_ids:
            return set()
        query = """
        SELECT sample_id
        FROM label_studio_tasks
        WHERE project_id = %s
          AND sample_id = ANY(%s)
        """
        with connect(self.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (project_id, sample_ids))
                rows = cur.fetchall()
        return {row["sample_id"] for row in rows}

    def record_exported_task(self, sample_id: str, project_id: int, task_id: int) -> None:
        query = """
        INSERT INTO label_studio_tasks (sample_id, project_id, task_id, exported_at, imported_annotation_keys)
        VALUES (%s, %s, %s, NOW(), '[]'::jsonb)
        ON CONFLICT (sample_id) DO UPDATE
        SET project_id = EXCLUDED.project_id,
            task_id = EXCLUDED.task_id
        """
        with connect(self.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (sample_id, project_id, task_id))
            conn.commit()

    def imported_annotation_keys(self, sample_id: str, project_id: int) -> set[str]:
        query = """
        SELECT imported_annotation_keys
        FROM label_studio_tasks
        WHERE sample_id = %s
          AND project_id = %s
        """
        with connect(self.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (sample_id, project_id))
                row = cur.fetchone()
        return set(row["imported_annotation_keys"] or []) if row else set()

    def add_imported_annotation_key(self, sample_id: str, project_id: int, key: str) -> None:
        existing = self.imported_annotation_keys(sample_id, project_id)
        existing.add(key)
        query = """
        INSERT INTO label_studio_tasks (
          sample_id,
          project_id,
          task_id,
          exported_at,
          last_imported_at,
          imported_annotation_keys
        ) VALUES (%s, %s, 0, NOW(), NOW(), %s)
        ON CONFLICT (sample_id) DO UPDATE
        SET last_imported_at = NOW(),
            imported_annotation_keys = EXCLUDED.imported_annotation_keys
        """
        with connect(self.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (sample_id, project_id, Json(sorted(existing))))
            conn.commit()


def _build_state_store(cfg: SyncConfig) -> SyncStateStore:
    pg_dsn = os.getenv("PG_DSN")
    if not pg_dsn:
        return LocalJsonStateStore(cfg.state_path)
    store = PostgresSyncStateStore(pg_dsn)
    try:
        store.exported_sample_ids(cfg.label_studio_project_id, ["__label_studio_sync_probe__"])
    except PsycopgError:
        return LocalJsonStateStore(cfg.state_path)
    return store


def _to_task_payload(candidate: dict[str, Any], cfg: SyncConfig) -> dict[str, Any]:
    return {
        "project": cfg.label_studio_project_id,
        "allow_skip": True,
        "data": {
            "image": f"{cfg.public_image_base_url}{candidate['object_url']}",
            "sample_id": candidate["sample_id"],
            "device_id": candidate["device_id"],
            "captured_at": candidate["captured_at"],
            "uncertainty_score": candidate.get("uncertainty_score"),
        }
    }


def _status_from_ls_annotation(annotation: dict[str, Any]) -> str:
    was_cancelled = bool(annotation.get("was_cancelled"))
    ground_truth = bool(annotation.get("ground_truth"))
    if was_cancelled:
        return "rejected"
    if ground_truth:
        return "verified"
    return "reviewed"


def _annotation_key(task_id: Any, annotation: dict[str, Any]) -> str:
    return f"{task_id}:{annotation.get('id')}"


def _to_backend_annotation(task: dict[str, Any], annotation: dict[str, Any]) -> dict[str, Any]:
    data = task.get("data", {})
    sample_id = data.get("sample_id")
    if not sample_id:
        raise SyncError(f"task {task.get('id')} missing data.sample_id")

    completed_by = annotation.get("completed_by")
    if isinstance(completed_by, dict):
        annotator_id = completed_by.get("email") or completed_by.get("username") or str(completed_by.get("id", "unknown"))
    else:
        annotator_id = str(completed_by) if completed_by is not None else "unknown"

    reviewed_at = annotation.get("completed_at") or annotation.get("updated_at") or task.get("updated_at") or datetime.now(UTC).isoformat()
    return {
        "annotation_id": str(uuid.uuid4()),
        "sample_id": sample_id,
        "tool_name": "label-studio",
        "annotator_id": annotator_id,
        "status": _status_from_ls_annotation(annotation),
        "labels": {"raw_result": annotation.get("result", [])},
        "quality_score": None,
        "reviewed_at": reviewed_at,
    }


def run_export(
    cfg: SyncConfig,
    *,
    backend_client: BackendClient | None = None,
    label_studio_client: LabelStudioClient | None = None,
    state_store: SyncStateStore | None = None,
) -> dict[str, int]:
    backend_client = backend_client or BackendClient(cfg)
    label_studio_client = label_studio_client or LabelStudioClient(cfg)
    state_store = state_store or _build_state_store(cfg)
    candidates = backend_client.get_review_candidates()
    existing_sample_ids = state_store.exported_sample_ids(cfg.label_studio_project_id, [c["sample_id"] for c in candidates])

    exported = 0
    claimed = 0
    skipped_existing = 0
    failed = 0

    for candidate in candidates:
        sample_id = candidate["sample_id"]
        if sample_id in existing_sample_ids:
            skipped_existing += 1
            continue

        try:
            backend_client.claim_sample(sample_id)
            claimed += 1
        except (SyncError, PsycopgError):
            failed += 1
            continue

        try:
            task = label_studio_client.create_task(_to_task_payload(candidate, cfg))
            state_store.record_exported_task(sample_id, cfg.label_studio_project_id, int(task["id"]))
            exported += 1
        except (SyncError, PsycopgError):
            failed += 1

    return {
        "fetched": len(candidates),
        "claimed": claimed,
        "exported": exported,
        "skipped_existing": skipped_existing,
        "failed": failed,
    }


def run_import(
    cfg: SyncConfig,
    *,
    backend_client: BackendClient | None = None,
    label_studio_client: LabelStudioClient | None = None,
    state_store: SyncStateStore | None = None,
) -> dict[str, int]:
    backend_client = backend_client or BackendClient(cfg)
    label_studio_client = label_studio_client or LabelStudioClient(cfg)
    state_store = state_store or _build_state_store(cfg)

    inspected = 0
    imported = 0
    skipped_duplicate = 0
    failed = 0
    task_count = 0

    for task in label_studio_client.iter_annotated_tasks():
        task_count += 1
        sample_id = task.get("data", {}).get("sample_id")
        annotations = task.get("annotations", [])
        for annotation in annotations:
            inspected += 1
            key = _annotation_key(task.get("id"), annotation)
            try:
                if not sample_id:
                    raise SyncError(f"task {task.get('id')} missing data.sample_id")
                if key in state_store.imported_annotation_keys(sample_id, cfg.label_studio_project_id):
                    skipped_duplicate += 1
                    continue
                payload = _to_backend_annotation(task, annotation)
                backend_client.write_annotation(payload)
                state_store.add_imported_annotation_key(sample_id, cfg.label_studio_project_id, key)
                imported += 1
            except (SyncError, PsycopgError):
                failed += 1

    return {
        "tasks": task_count,
        "annotations_inspected": inspected,
        "imported": imported,
        "skipped_duplicate": skipped_duplicate,
        "failed": failed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync queue and annotations with Label Studio")
    parser.add_argument("command", choices=["export", "import"])
    args = parser.parse_args()

    cfg = _load_config()
    if args.command == "export":
        summary = run_export(cfg)
    else:
        summary = run_import(cfg)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    try:
        main()
    except SyncError as exc:
        print(json.dumps({"status": "error", "message": str(exc)}), file=sys.stderr)
        raise SystemExit(1)
