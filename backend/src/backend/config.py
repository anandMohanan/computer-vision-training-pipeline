import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    s3_endpoint_url: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_bucket: str
    s3_region: str
    max_request_bytes: int
    max_metadata_bytes: int
    max_image_bytes: int
    auto_create_s3_bucket: bool
    log_level: str
    metrics_persist_enabled: bool


def _must_get(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=False)

    return Settings(
        pg_dsn=_must_get("PG_DSN"),
        s3_endpoint_url=_must_get("S3_ENDPOINT_URL"),
        s3_access_key_id=_must_get("S3_ACCESS_KEY_ID"),
        s3_secret_access_key=_must_get("S3_SECRET_ACCESS_KEY"),
        s3_bucket=_must_get("S3_BUCKET"),
        s3_region=os.getenv("S3_REGION", "us-east-1"),
        max_request_bytes=int(os.getenv("MAX_REQUEST_BYTES", str(12 * 1024 * 1024))),
        max_metadata_bytes=int(os.getenv("MAX_METADATA_BYTES", str(256 * 1024))),
        max_image_bytes=int(os.getenv("MAX_IMAGE_BYTES", str(10 * 1024 * 1024))),
        auto_create_s3_bucket=_bool_env("AUTO_CREATE_S3_BUCKET", False),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        metrics_persist_enabled=_bool_env("METRICS_PERSIST_ENABLED", True),
    )
