import json
import time
import uuid
from datetime import UTC
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from pydantic import ValidationError
from psycopg import connect
from psycopg.errors import Error as PsycopgError
from psycopg.rows import dict_row
from psycopg.types.json import Json

from backend.services import (
    DB_LATENCY,
    INGEST_DUPLICATE,
    INGEST_FAILURE,
    INGEST_SUCCESS,
    S3_LATENCY,
    ERROR_RESPONSES,
    SampleMetadata,
    get_s3_client,
    persist_metric_event,
    settings,
)

router = APIRouter()


@router.get("/samples/{object_path:path}", responses=ERROR_RESPONSES)
def get_sample_image(object_path: str) -> Response:
    object_key = f"samples/{object_path.lstrip('/')}"
    try:
        s3 = get_s3_client()
        timer = time.perf_counter()
        result = s3.get_object(Bucket=settings.s3_bucket, Key=object_key)
        S3_LATENCY.labels(operation="get_object").observe(time.perf_counter() - timer)
    except (ClientError, BotoCoreError) as exc:
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if error_code == "NoSuchKey":
            raise HTTPException(status_code=404, detail="image not found") from exc
        raise HTTPException(status_code=502, detail=f"failed to fetch image from S3: {exc}") from exc

    image_bytes = result["Body"].read()
    content_type = result.get("ContentType") or "application/octet-stream"
    return Response(content=image_bytes, media_type=content_type)


@router.get("/v1/samples/{sample_id}", responses=ERROR_RESPONSES)
def get_sample(sample_id: str) -> dict[str, Any]:
    query = """
    SELECT
      sample_id,
      device_id,
      captured_at,
      received_at,
      model_name,
      model_version,
      model_format,
      object_key,
      filter_decision,
      selection_reason,
      uncertainty_score,
      detections,
      metadata
    FROM samples
    WHERE sample_id = %s
    """

    try:
        timer = time.perf_counter()
        with connect(settings.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (sample_id,))
                row = cur.fetchone()
        DB_LATENCY.labels(operation="select_sample").observe(time.perf_counter() - timer)
    except PsycopgError as exc:
        raise HTTPException(status_code=502, detail=f"failed to fetch sample: {exc}") from exc

    if not row:
        raise HTTPException(status_code=404, detail="sample not found")

    return {
        "sample_id": row["sample_id"],
        "device_id": row["device_id"],
        "captured_at": row["captured_at"].isoformat(),
        "received_at": row["received_at"].isoformat(),
        "model": {
            "name": row["model_name"],
            "version": row["model_version"],
            "format": row["model_format"],
        },
        "object_key": row["object_key"],
        "object_url": f"/samples/{row['object_key'].removeprefix('samples/')}",
        "filter_decision": row["filter_decision"],
        "selection_reason": row["selection_reason"],
        "uncertainty_score": row["uncertainty_score"],
        "detections": row["detections"],
        "metadata": row["metadata"],
    }


@router.post("/v1/samples", responses=ERROR_RESPONSES)
async def ingest_sample(request: Request, image: UploadFile = File(...), metadata: str = Form(...)) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    s3_latency_ms: float | None = None
    db_latency_ms: float | None = None

    if len(metadata.encode("utf-8")) > settings.max_metadata_bytes:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=None, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message="metadata too large")
        raise HTTPException(status_code=400, detail=f"metadata exceeds limit of {settings.max_metadata_bytes} bytes")

    if image.content_type not in {"image/jpeg", "image/jpg"}:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=None, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message="invalid image content type")
        raise HTTPException(status_code=400, detail="image must be JPEG")

    try:
        raw_metadata = json.loads(metadata)
        parsed = SampleMetadata.model_validate(raw_metadata)
    except json.JSONDecodeError as exc:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=None, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message=f"invalid metadata JSON: {exc.msg}")
        raise HTTPException(status_code=400, detail=f"invalid metadata JSON: {exc.msg}") from exc
    except ValidationError as exc:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=str(raw_metadata.get("sample_id")) if isinstance(raw_metadata, dict) else None, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message="metadata validation failed")
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    sample_id = parsed.sample_id
    image_bytes = await image.read()
    if not image_bytes:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=sample_id, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message="image file is empty")
        raise HTTPException(status_code=400, detail="image file is empty")
    if len(image_bytes) > settings.max_image_bytes:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=sample_id, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message="image too large")
        raise HTTPException(status_code=400, detail=f"image exceeds limit of {settings.max_image_bytes} bytes")

    captured_at = parsed.timestamp.astimezone(UTC)
    object_key = f"samples/{captured_at:%Y/%m/%d}/{parsed.device_id}/{sample_id}.jpg"

    try:
        s3 = get_s3_client()
        timer = time.perf_counter()
        s3.put_object(Bucket=settings.s3_bucket, Key=object_key, Body=image_bytes, ContentType="image/jpeg")
        s3_latency_ms = round((time.perf_counter() - timer) * 1000, 3)
        S3_LATENCY.labels(operation="put_object").observe(s3_latency_ms / 1000)
    except (ClientError, BotoCoreError) as exc:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=sample_id, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message=f"failed to store image in S3: {exc}")
        raise HTTPException(status_code=502, detail=f"failed to store image in S3: {exc}") from exc

    insert_sql = """
    INSERT INTO samples (
      sample_id, device_id, captured_at, model_name, model_version, model_format,
      object_key, filter_decision, selection_reason, uncertainty_score, detections, metadata
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (sample_id) DO NOTHING
    RETURNING sample_id;
    """
    select_sql = "SELECT sample_id, object_key FROM samples WHERE sample_id = %s"

    try:
        timer = time.perf_counter()
        with connect(settings.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_sql, (sample_id, parsed.device_id, captured_at, parsed.model.name, parsed.model.version, parsed.model.format, object_key, parsed.filter_decision, parsed.selection_reason, parsed.uncertainty_score, Json([d.model_dump() for d in parsed.detections]), Json(parsed.model_dump(mode="json"))))
                inserted = cur.fetchone()
                if inserted:
                    conn.commit()
                    db_latency_ms = round((time.perf_counter() - timer) * 1000, 3)
                    DB_LATENCY.labels(operation="insert_sample").observe(db_latency_ms / 1000)
                    INGEST_SUCCESS.inc()
                    persist_metric_event(request_id=request_id, sample_id=sample_id, event_type="ingest", status="success", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message=None)
                    return {"status": "accepted", "sample_id": sample_id, "object_key": object_key, "object_url": f"/samples/{object_key.removeprefix('samples/')}"}

                cur.execute(select_sql, (sample_id,))
                existing = cur.fetchone()
                conn.commit()

        db_latency_ms = round((time.perf_counter() - timer) * 1000, 3)
        DB_LATENCY.labels(operation="insert_sample").observe(db_latency_ms / 1000)
        INGEST_DUPLICATE.inc()
        persist_metric_event(request_id=request_id, sample_id=sample_id, event_type="ingest", status="duplicate", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message=None)
        key = existing["object_key"] if existing else object_key
        return {"status": "already_exists", "sample_id": sample_id, "object_key": key, "object_url": f"/samples/{key.removeprefix('samples/')}"}
    except PsycopgError as exc:
        INGEST_FAILURE.inc()
        persist_metric_event(request_id=request_id, sample_id=sample_id, event_type="ingest", status="failed", s3_latency_ms=s3_latency_ms, db_latency_ms=db_latency_ms, error_message=f"failed to persist metadata: {exc}")
        raise HTTPException(status_code=502, detail=f"failed to persist metadata: {exc}") from exc
