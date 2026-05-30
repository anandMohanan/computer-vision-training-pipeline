"""add label studio sync state

Revision ID: 20260530_0005
Revises: 20260527_0004
Create Date: 2026-05-30
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260530_0005"
down_revision: Union[str, Sequence[str], None] = "20260527_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


OWNER_ROLE = "yolo"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS label_studio_tasks (
          sample_id TEXT PRIMARY KEY REFERENCES samples(sample_id) ON DELETE CASCADE,
          project_id INTEGER NOT NULL,
          task_id BIGINT NOT NULL,
          exported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_imported_at TIMESTAMPTZ,
          imported_annotation_keys JSONB NOT NULL DEFAULT '[]'::jsonb
        )
        """
    )
    op.execute(f"ALTER TABLE label_studio_tasks OWNER TO {OWNER_ROLE}")
    op.execute("CREATE INDEX IF NOT EXISTS idx_label_studio_tasks_project ON label_studio_tasks(project_id)")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_label_studio_tasks_task ON label_studio_tasks(project_id, task_id) WHERE task_id > 0"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_label_studio_tasks_task")
    op.execute("DROP INDEX IF EXISTS idx_label_studio_tasks_project")
    op.execute("DROP TABLE IF EXISTS label_studio_tasks")
