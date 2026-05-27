import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg import connect
from psycopg.errors import Error as PsycopgError
from psycopg.rows import dict_row

from backend.services import DB_LATENCY, ERROR_RESPONSES, QueueClaimRequest, select_review_candidates, settings

router = APIRouter()


@router.get("/v1/queue/review-candidates", responses=ERROR_RESPONSES)
def get_review_candidates(limit: int = Query(default=50, ge=1, le=500), per_device_cap: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    try:
        candidates = select_review_candidates(limit=limit, per_device_cap=per_device_cap)
    except PsycopgError as exc:
        raise HTTPException(status_code=502, detail=f"failed to fetch review candidates: {exc}") from exc
    return {"count": len(candidates), "candidates": candidates}


@router.post("/v1/queue/claim", responses=ERROR_RESPONSES)
def claim_queue_sample(payload: QueueClaimRequest, request: Request) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    ttl_seconds = payload.ttl_seconds if payload.ttl_seconds and payload.ttl_seconds > 0 else 900

    query = """
    UPDATE samples
    SET claimed_by = %s,
        claimed_at = NOW(),
        claim_expires_at = NOW() + (%s || ' seconds')::interval
    WHERE sample_id = %s
      AND is_annotated = FALSE
      AND annotation_status = 'pending'
      AND (claim_expires_at IS NULL OR claim_expires_at <= NOW())
    RETURNING sample_id, claimed_by, claimed_at, claim_expires_at
    """

    try:
        timer = time.perf_counter()
        with connect(settings.pg_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (payload.claimed_by, str(ttl_seconds), payload.sample_id))
                row = cur.fetchone()
            conn.commit()
        DB_LATENCY.labels(operation="claim_queue_sample").observe(time.perf_counter() - timer)
    except PsycopgError as exc:
        raise HTTPException(status_code=502, detail=f"failed to claim sample: {exc}") from exc

    if not row:
        raise HTTPException(status_code=400, detail="sample is not claimable")

    return {
        "status": "claimed",
        "sample_id": row["sample_id"],
        "claimed_by": row["claimed_by"],
        "claimed_at": row["claimed_at"].isoformat(),
        "claim_expires_at": row["claim_expires_at"].isoformat(),
        "request_id": request_id,
    }
