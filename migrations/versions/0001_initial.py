"""Initial Syncopy schema."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

numeric = sa.Numeric(30, 12)


def upgrade() -> None:
    op.create_table(
        "markets",
        sa.Column("token_id", sa.String(80), primary_key=True),
        sa.Column("condition_id", sa.String(70), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("tick_size", numeric, nullable=False),
        sa.Column("min_size", numeric, nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True)),
        sa.Column("neg_risk", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_markets_condition_id", "markets", ["condition_id"])
    op.create_table(
        "targets",
        sa.Column("address", sa.String(42), primary_key=True),
        sa.Column("label", sa.String(200), nullable=False),
        sa.Column("est_capital", numeric, nullable=False),
        sa.Column("weight", numeric, nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
    )
    op.create_table(
        "target_positions",
        sa.Column("address", sa.String(42), sa.ForeignKey("targets.address"), primary_key=True),
        sa.Column("token_id", sa.String(80), primary_key=True),
        sa.Column("size", numeric, nullable=False),
        sa.Column("avg_price", numeric, nullable=False),
        sa.Column("current_price", numeric),
        sa.Column("snapshot_at", sa.DateTime(timezone=True), primary_key=True),
    )
    op.create_index("ix_target_positions_latest", "target_positions", ["address", "snapshot_at"])
    op.create_table(
        "own_positions",
        sa.Column("token_id", sa.String(80), primary_key=True),
        sa.Column("size", numeric, nullable=False),
        sa.Column("avg_price", numeric, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "orders",
        sa.Column("client_id", sa.String(64), primary_key=True),
        sa.Column("exchange_order_id", sa.String(160), unique=True),
        sa.Column("token_id", sa.String(80), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("size", numeric, nullable=False),
        sa.Column("price", numeric, nullable=False),
        sa.Column("desired_position", numeric, nullable=False),
        sa.Column("current_position", numeric, nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("raw_response", postgresql.JSONB()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_orders_token_id", "orders", ["token_id"])
    op.create_index("ix_orders_status", "orders", ["status"])
    op.create_table(
        "fills",
        sa.Column("fill_id", sa.String(160), primary_key=True),
        sa.Column("client_id", sa.String(64), sa.ForeignKey("orders.client_id"), nullable=False),
        sa.Column("size", numeric, nullable=False),
        sa.Column("price", numeric, nullable=False),
        sa.Column("fee", numeric, nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_fills_client_id", "fills", ["client_id"])
    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(64), nullable=False),
        sa.Column("type", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
    )
    op.create_index("ix_events_ts", "events", ["ts"])
    op.create_index("ix_events_source", "events", ["source"])
    op.create_index("ix_events_type", "events", ["type"])
    op.create_table(
        "reconciliation_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scope", sa.String(160), nullable=False),
        sa.Column("before", postgresql.JSONB(), nullable=False),
        sa.Column("after", postgresql.JSONB(), nullable=False),
        sa.Column("divergence", sa.Text(), nullable=False),
    )
    op.create_index("ix_reconciliation_events_ts", "reconciliation_events", ["ts"])
    op.create_index("ix_reconciliation_events_scope", "reconciliation_events", ["scope"])
    op.create_table(
        "pnl_snapshots",
        sa.Column("ts", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("equity", numeric, nullable=False),
        sa.Column("realized", numeric, nullable=False),
        sa.Column("unrealized", numeric, nullable=False),
    )


def downgrade() -> None:
    for table in (
        "pnl_snapshots",
        "reconciliation_events",
        "events",
        "fills",
        "orders",
        "own_positions",
        "target_positions",
        "targets",
        "markets",
    ):
        op.drop_table(table)
