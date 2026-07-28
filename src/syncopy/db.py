from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from syncopy.domain import (
    ExecutionResult,
    Fill,
    MarketInfo,
    OrderIntent,
    OrderStatus,
    Position,
    utcnow,
)

DECIMAL = Numeric(30, 12)
JSON_TYPE = JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass


class MarketRow(Base):
    __tablename__ = "markets"

    token_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    condition_id: Mapped[str] = mapped_column(String(70), index=True)
    question: Mapped[str] = mapped_column(Text, default="")
    tick_size: Mapped[Decimal] = mapped_column(DECIMAL)
    min_size: Mapped[Decimal] = mapped_column(DECIMAL)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    neg_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class TargetRow(Base):
    __tablename__ = "targets"

    address: Mapped[str] = mapped_column(String(42), primary_key=True)
    label: Mapped[str] = mapped_column(String(200), default="")
    est_capital: Mapped[Decimal] = mapped_column(DECIMAL)
    weight: Mapped[Decimal] = mapped_column(DECIMAL, default=Decimal("1"))
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class TargetPositionRow(Base):
    __tablename__ = "target_positions"

    address: Mapped[str] = mapped_column(
        String(42), ForeignKey("targets.address"), primary_key=True
    )
    token_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    size: Mapped[Decimal] = mapped_column(DECIMAL)
    avg_price: Mapped[Decimal] = mapped_column(DECIMAL)
    current_price: Mapped[Decimal | None] = mapped_column(DECIMAL)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    __table_args__ = (Index("ix_target_positions_latest", "address", "snapshot_at"),)


class OwnPositionRow(Base):
    __tablename__ = "own_positions"

    token_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    size: Mapped[Decimal] = mapped_column(DECIMAL)
    avg_price: Mapped[Decimal] = mapped_column(DECIMAL)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class OrderRow(Base):
    __tablename__ = "orders"

    client_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(160), unique=True)
    token_id: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(4))
    size: Mapped[Decimal] = mapped_column(DECIMAL)
    price: Mapped[Decimal] = mapped_column(DECIMAL)
    desired_position: Mapped[Decimal] = mapped_column(DECIMAL)
    current_position: Mapped[Decimal] = mapped_column(DECIMAL)
    reason: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), index=True)
    error: Mapped[str | None] = mapped_column(Text)
    raw_response: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class FillRow(Base):
    __tablename__ = "fills"

    fill_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    client_id: Mapped[str] = mapped_column(String(64), ForeignKey("orders.client_id"), index=True)
    size: Mapped[Decimal] = mapped_column(DECIMAL)
    price: Mapped[Decimal] = mapped_column(DECIMAL)
    fee: Mapped[Decimal] = mapped_column(DECIMAL, default=Decimal("0"))
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class EventRow(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    source: Mapped[str] = mapped_column(String(64), index=True)
    type: Mapped[str] = mapped_column(String(100), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE)


class ReconciliationEventRow(Base):
    __tablename__ = "reconciliation_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    scope: Mapped[str] = mapped_column(String(160), index=True)
    before: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE)
    after: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE)
    divergence: Mapped[str] = mapped_column(Text)


class PnlSnapshotRow(Base):
    __tablename__ = "pnl_snapshots"

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    equity: Mapped[Decimal] = mapped_column(DECIMAL)
    realized: Mapped[Decimal] = mapped_column(DECIMAL)
    unrealized: Mapped[Decimal] = mapped_column(DECIMAL)


class Database:
    def __init__(self, url: str, *, echo: bool = False) -> None:
        self.engine: AsyncEngine = create_async_engine(url, echo=echo, pool_pre_ping=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def create_schema(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def ping(self) -> None:
        async with self.engine.connect() as connection:
            await connection.execute(select(1))

    async def close(self) -> None:
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessions() as session, session.begin():
            yield session


class Repository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def event(
        self,
        source: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        ts: datetime | None = None,
    ) -> None:
        async with self.database.session() as session:
            session.add(
                EventRow(ts=ts or utcnow(), source=source, type=event_type, payload=payload)
            )

    async def events(self, items: Sequence[tuple[datetime, str, str, dict[str, Any]]]) -> None:
        if not items:
            return
        async with self.database.session() as session:
            session.add_all(
                [
                    EventRow(ts=ts, source=source, type=event_type, payload=payload)
                    for ts, source, event_type, payload in items
                ]
            )

    async def reconciliation(
        self,
        scope: str,
        before: dict[str, Any],
        after: dict[str, Any],
        divergence: str,
    ) -> None:
        async with self.database.session() as session:
            session.add(
                ReconciliationEventRow(
                    scope=scope, before=before, after=after, divergence=divergence
                )
            )

    async def upsert_market(self, market: MarketInfo) -> None:
        async with self.database.session() as session:
            row = await session.get(MarketRow, market.token_id)
            if row is None:
                session.add(
                    MarketRow(
                        token_id=market.token_id,
                        condition_id=market.condition_id,
                        question=market.question,
                        tick_size=market.tick_size,
                        min_size=market.min_size,
                        active=market.active,
                        end_date=market.end_date,
                        neg_risk=market.neg_risk,
                    )
                )
            else:
                row.condition_id = market.condition_id
                row.question = market.question
                row.tick_size = market.tick_size
                row.min_size = market.min_size
                row.active = market.active
                row.end_date = market.end_date
                row.neg_risk = market.neg_risk

    async def upsert_target(
        self, address: str, capital: Decimal, weight: Decimal, label: str = ""
    ) -> None:
        async with self.database.session() as session:
            row = await session.get(TargetRow, address.lower())
            if row is None:
                session.add(
                    TargetRow(
                        address=address.lower(),
                        label=label,
                        est_capital=capital,
                        weight=weight,
                        active=True,
                    )
                )
            else:
                row.est_capital = capital
                row.weight = weight
                row.active = True
                if label:
                    row.label = label

    async def save_target_snapshot(
        self, address: str, positions: Sequence[Position], snapshot_at: datetime
    ) -> None:
        async with self.database.session() as session:
            session.add_all(
                [
                    TargetPositionRow(
                        address=address.lower(),
                        token_id=position.token_id,
                        size=position.size,
                        avg_price=position.avg_price,
                        current_price=position.current_price,
                        snapshot_at=snapshot_at,
                    )
                    for position in positions
                ]
            )

    async def replace_own_positions(self, positions: Sequence[Position]) -> None:
        async with self.database.session() as session:
            existing = {
                row.token_id: row for row in (await session.scalars(select(OwnPositionRow))).all()
            }
            incoming = {position.token_id: position for position in positions if position.size > 0}
            for token_id, row in existing.items():
                if token_id not in incoming:
                    await session.delete(row)
            for token_id, position in incoming.items():
                existing_row = existing.get(token_id)
                if existing_row is None:
                    session.add(
                        OwnPositionRow(
                            token_id=token_id,
                            size=position.size,
                            avg_price=position.avg_price,
                            updated_at=position.observed_at,
                        )
                    )
                else:
                    existing_row.size = position.size
                    existing_row.avg_price = position.avg_price
                    existing_row.updated_at = position.observed_at

    async def reserve_intent(self, intent: OrderIntent) -> bool:
        async with self.database.session() as session:
            if await session.get(OrderRow, intent.client_id) is not None:
                return False
            session.add(
                OrderRow(
                    client_id=intent.client_id,
                    token_id=intent.token_id,
                    side=intent.side.value,
                    size=intent.size,
                    price=intent.price,
                    desired_position=intent.desired_position,
                    current_position=intent.current_position,
                    reason=intent.reason,
                    status=OrderStatus.INTENT.value,
                    created_at=intent.created_at,
                    updated_at=intent.created_at,
                )
            )
            return True

    async def update_order(self, result: ExecutionResult) -> None:
        async with self.database.session() as session:
            row = await session.get(OrderRow, result.client_id)
            if row is None:
                raise LookupError(f"Unknown client id: {result.client_id}")
            row.exchange_order_id = result.exchange_order_id or row.exchange_order_id
            row.status = result.status.value
            row.error = result.message or None
            row.raw_response = result.raw
            row.updated_at = utcnow()

    async def save_fill(self, fill: Fill) -> bool:
        async with self.database.session() as session:
            if await session.get(FillRow, fill.fill_id) is None:
                session.add(
                    FillRow(
                        fill_id=fill.fill_id,
                        client_id=fill.client_id,
                        size=fill.size,
                        price=fill.price,
                        fee=fill.fee,
                        filled_at=fill.filled_at,
                    )
                )
                return True
            return False

    async def orders_by_exchange_ids(self, order_ids: Sequence[str]) -> list[OrderRow]:
        if not order_ids:
            return []
        async with self.database.sessions() as session:
            return list(
                await session.scalars(
                    select(OrderRow).where(OrderRow.exchange_order_id.in_(order_ids))
                )
            )

    async def open_orders_older_than(self, cutoff: datetime) -> list[OrderRow]:
        async with self.database.sessions() as session:
            result = await session.scalars(
                select(OrderRow).where(
                    OrderRow.status.in_(
                        [
                            OrderStatus.OPEN.value,
                            OrderStatus.PARTIALLY_FILLED.value,
                            OrderStatus.SUBMITTING.value,
                        ]
                    ),
                    OrderRow.updated_at < cutoff,
                )
            )
            return list(result)

    async def daily_pnl(self) -> Decimal:
        async with self.database.sessions() as session:
            latest = await session.scalar(
                select(PnlSnapshotRow).order_by(PnlSnapshotRow.ts.desc()).limit(1)
            )
            if latest is None:
                return Decimal("0")
            start = await session.scalar(
                select(PnlSnapshotRow)
                .where(func.date(PnlSnapshotRow.ts) == func.current_date())
                .order_by(PnlSnapshotRow.ts.asc())
                .limit(1)
            )
            return latest.equity - start.equity if start is not None else Decimal("0")

    async def save_pnl_snapshot(
        self,
        *,
        equity: Decimal,
        realized: Decimal,
        unrealized: Decimal,
        ts: datetime | None = None,
    ) -> None:
        async with self.database.session() as session:
            session.add(
                PnlSnapshotRow(
                    ts=ts or utcnow(),
                    equity=equity,
                    realized=realized,
                    unrealized=unrealized,
                )
            )

    async def load_own_positions(self) -> list[Position]:
        async with self.database.sessions() as session:
            rows = list(await session.scalars(select(OwnPositionRow)))
        return [
            Position(
                token_id=row.token_id,
                size=row.size,
                avg_price=row.avg_price,
                observed_at=row.updated_at,
            )
            for row in rows
        ]

    async def cumulative_cashflow(self) -> Decimal:
        """Net cash change represented by durable fills (buys negative, sells positive)."""

        async with self.database.sessions() as session:
            rows = (
                await session.execute(
                    select(FillRow.size, FillRow.price, FillRow.fee, OrderRow.side).join(
                        OrderRow, FillRow.client_id == OrderRow.client_id
                    )
                )
            ).all()
        return sum(
            (
                (size * price - fee) if side == "SELL" else -(size * price + fee)
                for size, price, fee, side in rows
            ),
            Decimal("0"),
        )

    async def iter_events(
        self, *, after: datetime | None = None, limit: int | None = None
    ) -> list[EventRow]:
        statement = select(EventRow).order_by(EventRow.ts, EventRow.id)
        if after is not None:
            statement = statement.where(EventRow.ts >= after)
        if limit is not None:
            statement = statement.limit(limit)
        async with self.database.sessions() as session:
            return list(await session.scalars(statement))
