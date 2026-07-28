from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from syncopy.domain import ZERO, Freshness, Position, PriceLevel, Side, utcnow


@dataclass(slots=True)
class OrderBook:
    token_id: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    freshness: Freshness = Freshness.STALE
    updated_at: datetime | None = None
    sequence_hash: str | None = None

    def replace(
        self,
        bids: Iterable[PriceLevel],
        asks: Iterable[PriceLevel],
        *,
        sequence_hash: str | None = None,
    ) -> None:
        self.bids = {level.price: level.size for level in bids if level.size > ZERO}
        self.asks = {level.price: level.size for level in asks if level.size > ZERO}
        self.freshness = Freshness.FRESH
        self.updated_at = utcnow()
        self.sequence_hash = sequence_hash

    def update_level(self, side: Side, price: Decimal, size: Decimal) -> None:
        levels = self.bids if side is Side.BUY else self.asks
        if size <= ZERO:
            levels.pop(price, None)
        else:
            levels[price] = size
        self.updated_at = utcnow()

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal("2")

    def levels_for(self, side: Side) -> list[PriceLevel]:
        if side is Side.BUY:
            return [PriceLevel(price, self.asks[price]) for price in sorted(self.asks)]
        return [PriceLevel(price, self.bids[price]) for price in sorted(self.bids, reverse=True)]

    def depth_within(self, side: Side, tolerance_pct: Decimal) -> Decimal:
        levels = self.levels_for(side)
        if not levels:
            return ZERO
        tolerance = tolerance_pct / Decimal("100")
        best = levels[0].price
        boundary = (
            best * (Decimal("1") + tolerance)
            if side is Side.BUY
            else best * (Decimal("1") - tolerance)
        )
        return sum(
            (
                level.size
                for level in levels
                if (level.price <= boundary if side is Side.BUY else level.price >= boundary)
            ),
            ZERO,
        )

    def simulate_fill(
        self, side: Side, shares: Decimal, limit_price: Decimal
    ) -> tuple[Decimal, Decimal | None]:
        return self._fill(side, shares, limit_price, consume=False)

    def consume_fill(
        self, side: Side, shares: Decimal, limit_price: Decimal
    ) -> tuple[Decimal, Decimal | None]:
        """Fill and decrement displayed depth until a new authoritative update arrives."""

        return self._fill(side, shares, limit_price, consume=True)

    def _fill(
        self,
        side: Side,
        shares: Decimal,
        limit_price: Decimal,
        *,
        consume: bool,
    ) -> tuple[Decimal, Decimal | None]:
        remaining = shares
        notional = ZERO
        filled = ZERO
        for level in self.levels_for(side):
            crosses = level.price <= limit_price if side is Side.BUY else level.price >= limit_price
            if not crosses:
                break
            amount = min(level.size, remaining)
            filled += amount
            notional += amount * level.price
            remaining -= amount
            if consume:
                book_side = Side.SELL if side is Side.BUY else Side.BUY
                self.update_level(book_side, level.price, level.size - amount)
            if remaining <= ZERO:
                break
        return filled, (notional / filled if filled > ZERO else None)


@dataclass(slots=True)
class ServiceHealth:
    started_at: datetime = field(default_factory=utcnow)
    last_target_poll: datetime | None = None
    last_reconciliation: datetime | None = None
    last_ws_message: datetime | None = None
    last_error: str | None = None
    trading_halted: bool = False
    halt_reason: str | None = None


class RuntimeState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.books: dict[str, OrderBook] = {}
        self.own_positions: dict[str, Position] = {}
        self.target_positions: dict[str, dict[str, Position]] = {}
        self.health = ServiceHealth()

    async def book(self, token_id: str) -> OrderBook:
        async with self._lock:
            return self.books.setdefault(token_id, OrderBook(token_id))

    async def mark_books_stale(self, token_ids: Iterable[str]) -> None:
        async with self._lock:
            for token_id in token_ids:
                self.books.setdefault(token_id, OrderBook(token_id)).freshness = Freshness.STALE

    async def replace_book(
        self,
        token_id: str,
        bids: Iterable[PriceLevel],
        asks: Iterable[PriceLevel],
        *,
        sequence_hash: str | None = None,
    ) -> None:
        async with self._lock:
            book = self.books.setdefault(token_id, OrderBook(token_id))
            book.replace(bids, asks, sequence_hash=sequence_hash)

    async def update_book_level(
        self, token_id: str, side: Side, price: Decimal, size: Decimal
    ) -> None:
        async with self._lock:
            book = self.books.setdefault(token_id, OrderBook(token_id))
            book.update_level(side, price, size)

    async def set_own_positions(self, positions: dict[str, Position]) -> None:
        async with self._lock:
            self.own_positions = positions

    async def set_target_positions(self, address: str, positions: dict[str, Position]) -> None:
        async with self._lock:
            self.target_positions[address.lower()] = positions

    async def adjust_own_position(self, token_id: str, delta: Decimal, price: Decimal) -> Position:
        async with self._lock:
            before = self.own_positions.get(token_id, Position(token_id=token_id, size=ZERO))
            after_size = before.size + delta
            if after_size <= ZERO:
                updated = Position(token_id=token_id, size=ZERO, avg_price=ZERO)
                self.own_positions.pop(token_id, None)
                return updated
            if delta > ZERO:
                total_cost = before.size * before.avg_price + delta * price
                avg_price = total_cost / after_size
            else:
                avg_price = before.avg_price
            updated = Position(token_id=token_id, size=after_size, avg_price=avg_price)
            self.own_positions[token_id] = updated
            return updated

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            fresh = sum(book.freshness is Freshness.FRESH for book in self.books.values())
            return {
                "books": len(self.books),
                "fresh_books": fresh,
                "stale_books": len(self.books) - fresh,
                "own_positions": len(self.own_positions),
                "targets": len(self.target_positions),
            }
