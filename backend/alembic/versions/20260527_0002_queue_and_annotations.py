"""add queue fields and annotations table

Revision ID: 20260527_0002
Revises: 20260527_0001
Create Date: 2026-05-27
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260527_0002"
down_revision: Union[str, Sequence[str], None] = "20260527_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


OWNER_ROLE = "yolo"


def upgrade() -> None:
    op.execute("ALTER TABLE samples ADD COLUMN IF NOT EXISTS is_annotated BOOLEAN")
    op.execute("ALTER TABLE samples ADD COLUMN IF NOT EXISTS annotation_status TEXT")

    op.execute("UPDATE samples SET is_annotated = FALSE WHERE is_annotated IS NULL")
    op.execute("UPDATE samples SET annotation_status = 'pending' WHERE annotation_status IS NULL")

    op.execute("ALTER TABLE samples ALTER COLUMN is_annotated SET DEFAULT FALSE")
    op.execute("ALTER TABLE samples ALTER COLUMN is_annotated SET NOT NULL")
    op.execute("ALTER TABLE samples ALTER COLUMN annotation_status SET DEFAULT 'pending'")
    op.execute("ALTER TABLE samples ALTER COLUMN annotation_status SET NOT NULL")

    op.execute("ALTER TABLE samples DROP CONSTRAINT IF EXISTS samples_annotation_status_check")
    op.execute(
        """
        ALTER TABLE samples
        ADD CONSTRAINT samples_annotation_status_check
        CHECK (annotation_status IN ('pending', 'reviewed', 'verified', 'rejected'))
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS annotations (
          annotation_id UUID PRIMARY KEY,
          sample_id TEXT NOT NULL REFERENCES samples(sample_id) ON DELETE CASCADE,
          tool_name TEXT NOT NULL,
          annotator_id TEXT,
          reviewed_at TIMESTAMPTZ,
          status TEXT NOT NULL,
          labels JSONB NOT NULL,
          quality_score DOUBLE PRECISION,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(f"ALTER TABLE annotations OWNER TO {OWNER_ROLE}")

    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_annotation_status ON samples(annotation_status, is_annotated, received_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_queue_priority ON samples(uncertainty_score DESC, captured_at DESC)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_annotations_sample ON annotations(sample_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_annotations_status ON annotations(status)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_annotations_status")
    op.execute("DROP INDEX IF EXISTS idx_annotations_sample")
    op.execute("DROP INDEX IF EXISTS idx_samples_queue_priority")
    op.execute("DROP INDEX IF EXISTS idx_samples_annotation_status")

    op.execute("DROP TABLE IF EXISTS annotations")

    op.execute("ALTER TABLE samples DROP CONSTRAINT IF EXISTS samples_annotation_status_check")
    op.execute("ALTER TABLE samples DROP COLUMN IF EXISTS annotation_status")
    op.execute("ALTER TABLE samples DROP COLUMN IF EXISTS is_annotated")
