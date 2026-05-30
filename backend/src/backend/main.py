import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from backend.label_studio_scheduler import LabelStudioScheduler
from backend.label_studio_sync import SyncError, _load_config
from backend.routes.annotations import router as annotations_router
from backend.routes.queue import router as queue_router
from backend.routes.samples import router as samples_router
from backend.routes.system import router as system_router
from backend.services import error_response, logger, run_startup_checks, settings


app = FastAPI(
    title="YOLO Pipeline Backend",
    version="0.3.0",
    docs_url="/swagger",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

label_studio_scheduler: LabelStudioScheduler | None = None


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = request_id

    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            size = int(content_length)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid Content-Length header") from exc
        if size > settings.max_request_bytes:
            raise HTTPException(
                status_code=400,
                detail=f"request payload exceeds limit of {settings.max_request_bytes} bytes",
            )

    started = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - started) * 1000, 3)
    response.headers["X-Request-ID"] = request_id

    logger.info(
        "request complete",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        },
    )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> Response:
    message = str(exc.detail) if isinstance(exc.detail, str) else "request failed"
    payload = error_response(
        status_code=exc.status_code,
        message=message,
        detail=None if isinstance(exc.detail, str) else exc.detail,
    )
    return Response(
        content=payload.model_dump_json(),
        media_type="application/json",
        status_code=payload.status_code,
        headers={"X-Request-ID": getattr(request.state, "request_id", "")},
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> Response:
    payload = error_response(status_code=422, message="request validation failed", detail=exc.errors())
    return Response(
        content=payload.model_dump_json(),
        media_type="application/json",
        status_code=422,
        headers={"X-Request-ID": getattr(request.state, "request_id", "")},
    )


@app.on_event("startup")
async def on_startup() -> None:
    global label_studio_scheduler
    run_startup_checks(settings)
    logger.info("startup checks passed")
    if not settings.label_studio_sync_enabled:
        return

    try:
        sync_cfg = _load_config()
    except SyncError as exc:
        logger.error(f"label studio scheduler not started: {exc}")
        return

    label_studio_scheduler = LabelStudioScheduler(
        sync_cfg,
        enabled=settings.label_studio_sync_enabled,
        interval_seconds=settings.label_studio_sync_interval_seconds,
        run_on_startup=settings.label_studio_sync_run_on_startup,
        run_export_enabled=settings.label_studio_sync_run_export,
        run_import_enabled=settings.label_studio_sync_run_import,
    )
    await label_studio_scheduler.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if label_studio_scheduler is not None:
        await label_studio_scheduler.stop()


app.include_router(system_router)
app.include_router(samples_router)
app.include_router(queue_router)
app.include_router(annotations_router)


def main() -> None:
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
