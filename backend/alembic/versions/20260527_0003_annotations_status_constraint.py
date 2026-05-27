"""annotations status constraint and history index

Revision ID: 20260527_0003
Revises: 20260527_0002
Create Date: 2026-05-27
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260527_0003"
down_revision: Union[str, Sequence[str], None] = "20260527_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE annotations DROP CONSTRAINT IF EXISTS annotations_status_check")
    op.execute(
        """
        ALTER TABLE annotations
        ADD CONSTRAINT annotations_status_check
        CHECK (status IN ('reviewed', 'verified', 'rejected'))
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_annotations_sample_created ON annotations(sample_id, created_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_annotations_sample_created")
    op.execute("ALTER TABLE annotations DROP CONSTRAINT IF EXISTS annotations_status_check")
