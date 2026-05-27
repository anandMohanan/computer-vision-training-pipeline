import time
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg import connect
from psycopg.errors import UniqueViolation
from psycopg.errors import Error as PsycopgError
from psycopg.rows import dict_row
from psycopg.types.json import Json

from backend.services import (
    AnnotationWriteRequest,
    DB_LATENCY,
    ERROR_RESPONSES,
    persist_metric_event,
    settings,
    validate_status_transition,
)

router = APIRouter()


@router.post("/v1/annotations", responses=ERROR_RESPONSES)
def write_annotation(payload: AnnotationWriteRequest, request: Request) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    db_latency_ms: float | None = None
    reviewed_at = payload.reviewed_at.astimezone(UTC) if payload.reviewed_at else datetime.now(UTC)

    select_sample_sql = """
    SELECT sample_id, annotation_status, is_annotated
    FROM samples
    WHERE sample_id = %s
    FOR UPDATE
    """
    insert_annotation_sql = """
    INSERT INTO annotations (
      annotation_id,
      sample_id,
      tool_name,
      annotator_id,
      reviewed_at,
      status,
      labels,
      quality_score
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    update_sample_sql = """
    UPDATE samples
    SET annotation_status = %s,
        is_annotated = %s,
        claimed_by = NULL,
        claimed_at = NULL,
        claim_expires_at = NULL
    WHERE sample_id = %s
    """

    try:
        timer = time.perf_counter()
        with connect(settings.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(select_sample_sql, (payload.sample_id,))
                sample = cur.fetchone()
                if not sample:
                    raise HTTPException(status_code=404, detail="sample not found")

                validate_status_transition(sample["annotation_status"], payload.status)
                is_annotated = payload.status in {"verified", "rejected"}

                cur.execute(
                    insert_annotation_sql,
                    (
                        str(payload.annotation_id),
                        payload.sample_id,
                        payload.tool_name,
                        payload.annotator_id,
                        reviewed_at,
                        payload.status,
                        Json(payload.labels),
                        payload.quality_score,
                    ),
                )
                cur.execute(update_sample_sql, (payload.status, is_annotated, payload.sample_id))
            conn.commit()

        db_latency_ms = round((time.perf_counter() - timer) * 1000, 3)
        DB_LATENCY.labels(operation="write_annotation").observe(db_latency_ms / 1000)
        persist_metric_event(request_id=request_id, sample_id=payload.sample_id, event_type="annotation_write", status="success", s3_latency_ms=None, db_latency_ms=db_latency_ms, error_message=None)

        return {
            "status": "accepted",
            "annotation_id": str(payload.annotation_id),
            "sample_id": payload.sample_id,
            "sample_annotation_status": payload.status,
            "is_annotated": payload.status in {"verified", "rejected"},
        }
    except HTTPException as exc:
        persist_metric_event(request_id=request_id, sample_id=payload.sample_id, event_type="annotation_write", status="failed", s3_latency_ms=None, db_latency_ms=db_latency_ms, error_message=str(exc.detail))
        raise
    except UniqueViolation as exc:
        persist_metric_event(request_id=request_id, sample_id=payload.sample_id, event_type="annotation_write", status="failed", s3_latency_ms=None, db_latency_ms=db_latency_ms, error_message="duplicate annotation_id")
        raise HTTPException(status_code=400, detail="annotation_id already exists") from exc
    except PsycopgError as exc:
        persist_metric_event(request_id=request_id, sample_id=payload.sample_id, event_type="annotation_write", status="failed", s3_latency_ms=None, db_latency_ms=db_latency_ms, error_message=f"failed to persist annotation: {exc}")
        raise HTTPException(status_code=502, detail=f"failed to persist annotation: {exc}") from exc


@router.get("/v1/annotations/{sample_id}", responses=ERROR_RESPONSES)
def get_annotation_history(sample_id: str, limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    sample_sql = "SELECT sample_id FROM samples WHERE sample_id = %s"
    query = """
    SELECT
      annotation_id,
      sample_id,
      tool_name,
      annotator_id,
      reviewed_at,
      status,
      labels,
      quality_score,
      created_at
    FROM annotations
    WHERE sample_id = %s
    ORDER BY created_at DESC
    LIMIT %s
    """

    try:
        timer = time.perf_counter()
        with connect(settings.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sample_sql, (sample_id,))
                sample = cur.fetchone()
                if not sample:
                    raise HTTPException(status_code=404, detail="sample not found")

                cur.execute(query, (sample_id, limit))
                rows = cur.fetchall()
        DB_LATENCY.labels(operation="select_annotations").observe(time.perf_counter() - timer)
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=502, detail=f"failed to fetch annotation history: {exc}") from exc

    history = []
    for row in rows:
        history.append(
            {
                "annotation_id": str(row["annotation_id"]),
                "sample_id": row["sample_id"],
                "tool_name": row["tool_name"],
                "annotator_id": row["annotator_id"],
                "reviewed_at": row["reviewed_at"].isoformat() if row["reviewed_at"] else None,
                "status": row["status"],
                "labels": row["labels"],
                "quality_score": row["quality_score"],
                "created_at": row["created_at"].isoformat(),
            }
        )
    return {"count": len(history), "annotations": history}
