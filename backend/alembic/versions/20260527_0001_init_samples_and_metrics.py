"""init samples and metrics tables

Revision ID: 20260527_0001
Revises: 
Create Date: 2026-05-27
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260527_0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


OWNER_ROLE = "yolo"


def upgrade() -> None:
    op.execute(f"ALTER SCHEMA public OWNER TO {OWNER_ROLE}")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS samples (
          sample_id TEXT PRIMARY KEY,
          device_id TEXT NOT NULL,
          captured_at TIMESTAMPTZ NOT NULL,
          received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          model_name TEXT NOT NULL,
          model_version TEXT NOT NULL,
          model_format TEXT,
          object_key TEXT NOT NULL,
          filter_decision TEXT NOT NULL,
          selection_reason TEXT,
          uncertainty_score DOUBLE PRECISION,
          detections JSONB NOT NULL,
          metadata JSONB NOT NULL
        );
        """
    )
    op.execute(f"ALTER TABLE samples OWNER TO {OWNER_ROLE}")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_device_time ON samples(device_id, captured_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_uncertainty ON samples(uncertainty_score DESC)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_metric_events (
          id BIGSERIAL PRIMARY KEY,
          request_id TEXT NOT NULL,
          sample_id TEXT,
          event_type TEXT NOT NULL,
          status TEXT NOT NULL,
          s3_latency_ms DOUBLE PRECISION,
          db_latency_ms DOUBLE PRECISION,
          error_message TEXT,
          observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    op.execute(f"ALTER TABLE ingest_metric_events OWNER TO {OWNER_ROLE}")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ingest_metric_events_time ON ingest_metric_events(observed_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_ingest_metric_events_sample ON ingest_metric_events(sample_id)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ingest_metric_events")
    op.execute("DROP TABLE IF EXISTS samples")
