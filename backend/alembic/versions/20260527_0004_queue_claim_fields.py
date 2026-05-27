"""add queue claim fields

Revision ID: 20260527_0004
Revises: 20260527_0003
Create Date: 2026-05-27
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260527_0004"
down_revision: Union[str, Sequence[str], None] = "20260527_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE samples ADD COLUMN IF NOT EXISTS claimed_by TEXT")
    op.execute("ALTER TABLE samples ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
    op.execute("ALTER TABLE samples ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_claim_state ON samples(annotation_status, is_annotated, claim_expires_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_samples_claim_state")
    op.execute("ALTER TABLE samples DROP COLUMN IF EXISTS claim_expires_at")
    op.execute("ALTER TABLE samples DROP COLUMN IF EXISTS claimed_at")
    op.execute("ALTER TABLE samples DROP COLUMN IF EXISTS claimed_by")
