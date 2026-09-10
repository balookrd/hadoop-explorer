"""Initial database schema for Hadoop Explorer Platform

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-09-10 10:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. active_sessions
    op.create_table(
        "active_sessions",
        sa.Column("session_id", sa.String(128), primary_key=True),
        sa.Column("username", sa.String(128), nullable=False, index=True),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False, index=True),
        sa.Column("last_activity_at", sa.Float(), nullable=False),
        sa.Column("client_ip", sa.String(64), nullable=True),
        sa.Column("user_agent", sa.String(512), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
    )

    # 2. revoked_tokens
    op.create_table(
        "revoked_tokens",
        sa.Column("token_hash", sa.String(64), primary_key=True),
        sa.Column("jti", sa.String(128), nullable=True, index=True),
        sa.Column("revoked_at", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=True, index=True),
        sa.Column("username", sa.String(128), nullable=True, index=True),
        sa.Column("reason", sa.String(256), nullable=True),
    )

    # 3. rate_limits
    op.create_table(
        "rate_limits",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("key", sa.String(256), nullable=False, index=True),
        sa.Column("timestamp", sa.Float(), nullable=False, index=True),
    )

    # 4. distributed_locks
    op.create_table(
        "distributed_locks",
        sa.Column("key", sa.String(256), primary_key=True),
        sa.Column("owner_id", sa.String(256), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False, index=True),
        sa.Column("acquired_at", sa.Float(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("distributed_locks")
    op.drop_table("rate_limits")
    op.drop_table("revoked_tokens")
    op.drop_table("active_sessions")
