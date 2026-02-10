"""Add expiration support to normal honors

Revision ID: f3c1e2d4a5b6
Revises: 098296053a4b
Create Date: 2026-02-09 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f3c1e2d4a5b6"
down_revision: Union[str, Sequence[str], None] = "098296053a4b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # SQLite 兼容：使用 batch_alter_table
    with op.batch_alter_table("honor_definitions", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "expire_after_days",
                sa.Integer(),
                nullable=True,
                comment="获得后 N 天过期；NULL=永不过期",
            )
        )

    with op.batch_alter_table("user_honors", schema=None) as batch_op:
        batch_op.add_column(sa.Column("expires_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("user_honors", schema=None) as batch_op:
        batch_op.drop_column("expires_at")

    with op.batch_alter_table("honor_definitions", schema=None) as batch_op:
        batch_op.drop_column("expire_after_days")
