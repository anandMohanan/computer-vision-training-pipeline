import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from prometheus_client import Counter, Histogram
from pydantic import UUID4, BaseModel
from psycopg import connect
from psycopg.errors import Error as PsycopgError
from psycopg.rows import dict_row

from backend.config import Settings, load_settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "request_id",
            "sample_id",
            "annotation_id",
            "annotation_status",
            "method",
            "path",
            "status_code",
            "duration_ms",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, default=str)


class ErrorResponse(BaseModel):
    status_code: int
    error: str
    message: str
    detail: Any | None = None


class ModelInfo(BaseModel):
    name: str
    version: str
    format: str | None = None


class Detection(BaseModel):
    class_id: int
    class_name: str
    confidence: float
    bbox_xywh: list[float]


class SampleMetadata(BaseModel):
    sample_id: str
    device_id: str
    timestamp: datetime
    model: ModelInfo
    filter_decision: str
    selection_reason: str | None = None
    uncertainty_score: float | None = None
    detections: list[Detection] = []


class AnnotationWriteRequest(BaseModel):
    annotation_id: UUID4
    sample_id: str
    tool_name: str
    annotator_id: str | None = None
    status: str
    labels: dict[str, Any] | list[Any]
    quality_score: float | None = None
    reviewed_at: datetime | None = None


class QueueClaimRequest(BaseModel):
    sample_id: str
    claimed_by: str
    ttl_seconds: int | None = 900


settings = load_settings()
logger = logging.getLogger("backend")
if not logger.handlers:
    logger.setLevel(settings.log_level.upper())
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False

ALLOWED_ERROR_STATUS = {400, 404, 422, 502}
ERROR_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Bad request"},
    404: {"model": ErrorResponse, "description": "Not found"},
    422: {"model": ErrorResponse, "description": "Validation error"},
    502: {"model": ErrorResponse, "description": "Upstream dependency failure"},
}

INGEST_SUCCESS = Counter("backend_ingest_success_total", "Ingest success count")
INGEST_FAILURE = Counter("backend_ingest_failure_total", "Ingest failure count")
INGEST_DUPLICATE = Counter("backend_ingest_duplicate_total", "Duplicate ingest count")
S3_LATENCY = Histogram("backend_s3_latency_seconds", "S3 operation latency seconds", ["operation"])
DB_LATENCY = Histogram("backend_db_latency_seconds", "Database operation latency seconds", ["operation"])


def error_response(status_code: int, message: str, detail: Any | None = None) -> ErrorResponse:
    if status_code not in ALLOWED_ERROR_STATUS:
        status_code = 400
    return ErrorResponse(status_code=status_code, error=f"HTTP_{status_code}", message=message, detail=detail)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4"),
    )


def persist_metric_event(
    *,
    request_id: str,
    sample_id: str | None,
    event_type: str,
    status: str,
    s3_latency_ms: float | None,
    db_latency_ms: float | None,
    error_message: str | None,
) -> None:
    if not settings.metrics_persist_enabled:
        return

    insert_sql = """
    INSERT INTO ingest_metric_events (
      request_id,
      sample_id,
      event_type,
      status,
      s3_latency_ms,
      db_latency_ms,
      error_message,
      observed_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """
    try:
        with connect(settings.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (request_id, sample_id, event_type, status, s3_latency_ms, db_latency_ms, error_message),
                )
            conn.commit()
    except PsycopgError as exc:
        logger.error("failed to persist metric event", extra={"request_id": request_id, "sample_id": sample_id, "error": str(exc)})


def run_startup_checks(cfg: Settings) -> None:
    try:
        with connect(cfg.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except PsycopgError as exc:
        raise RuntimeError(f"startup check failed: postgres unavailable: {exc}") from exc

    s3 = get_s3_client()
    try:
        s3.list_buckets()
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"startup check failed: minio/s3 unavailable: {exc}") from exc

    try:
        s3.head_bucket(Bucket=cfg.s3_bucket)
    except (ClientError, BotoCoreError) as exc:
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchBucket", "NotFound"} and cfg.auto_create_s3_bucket:
            s3.create_bucket(Bucket=cfg.s3_bucket)
            return
        raise RuntimeError(
            "startup check failed: bucket does not exist or is inaccessible; set AUTO_CREATE_S3_BUCKET=true to auto-create"
        ) from exc


def sample_to_review_candidate(row: dict[str, Any]) -> dict[str, Any]:
    object_key = row["object_key"]
    return {
        "sample_id": row["sample_id"],
        "device_id": row["device_id"],
        "captured_at": row["captured_at"].isoformat(),
        "uncertainty_score": row["uncertainty_score"],
        "object_key": object_key,
        "object_url": f"/samples/{object_key.removeprefix('samples/')}",
        "annotation_status": row["annotation_status"],
        "claimed_by": row.get("claimed_by"),
        "claim_expires_at": row["claim_expires_at"].isoformat() if row.get("claim_expires_at") else None,
    }


def select_review_candidates(limit: int, per_device_cap: int) -> list[dict[str, Any]]:
    query = """
    WITH ranked AS (
      SELECT
        sample_id,
        device_id,
        captured_at,
        uncertainty_score,
        object_key,
        annotation_status,
        claimed_by,
        claim_expires_at,
        ROW_NUMBER() OVER (
          PARTITION BY device_id
          ORDER BY uncertainty_score DESC, captured_at DESC
        ) AS device_rank
      FROM samples
      WHERE is_annotated = FALSE
        AND annotation_status = 'pending'
        AND uncertainty_score IS NOT NULL
        AND (claim_expires_at IS NULL OR claim_expires_at <= NOW())
    )
    SELECT
      sample_id,
      device_id,
      captured_at,
      uncertainty_score,
      object_key,
      annotation_status,
      claimed_by,
      claim_expires_at
    FROM ranked
    WHERE device_rank <= %s
    ORDER BY uncertainty_score DESC, captured_at DESC
    LIMIT %s
    """
    timer = time.perf_counter()
    with connect(settings.pg_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (per_device_cap, limit))
            rows = cur.fetchall()
    DB_LATENCY.labels(operation="select_review_candidates").observe(time.perf_counter() - timer)
    return [sample_to_review_candidate(row) for row in rows]


def validate_status_transition(current: str, target: str) -> None:
    allowed_targets = {
        "pending": {"reviewed", "rejected"},
        "reviewed": {"verified", "rejected"},
    }
    if target not in {"reviewed", "verified", "rejected"}:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail="invalid target status")
    if target not in allowed_targets.get(current, set()):
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=f"invalid status transition: {current} -> {target}")
