from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, Protocol

import structlog

from syncopy.config import Settings
from syncopy.domain import ZERO, Freshness, Side, SignalEvent, SizingInput, TargetPositionChange
from syncopy.state import RuntimeState
from syncopy.strategy import PositionSizer

log = structlog.get_logger()

SignalCallback = Callable[[SignalEvent], Awaitable[None]]


class SignalSource(Protocol):
    async def run(self) -> None: ...
    async def stop(self) -> None: ...


class CopyTradingSignalSource:
    """Encapsulates the copy-trading sizing logic.

    Receives TargetPositionChange events, computes desired position size, and
    fires a SignalEvent through the registered callback.
    """

    def __init__(
        self,
        state: RuntimeState,
        settings: Settings,
        token_locks: dict[str, asyncio.Lock],
        get_ingestor: Callable[[], Any],
        get_markets: Callable[[], Any],
        get_reconciler: Callable[[], Any],
        get_events: Callable[[], Any],
        on_signal: SignalCallback,
    ) -> None:
        self.state = state
        self.settings = settings
        self._token_locks = token_locks
        self._get_ingestor = get_ingestor
        self._get_markets = get_markets
        self._get_reconciler = get_reconciler
        self._get_events = get_events
        self.on_signal = on_signal

    async def on_target_change(self, change: TargetPositionChange) -> None:
        ingestor = self._get_ingestor()
        markets = self._get_markets()
        reconciler = self._get_reconciler()
        events = self._get_events()
        assert ingestor is not None
        assert reconciler is not None
        assert markets is not None
        assert events is not None
        ingestor.add_assets([change.token_id])
        lock = self._token_locks.setdefault(change.token_id, asyncio.Lock())
        async with lock:
            reference = next(
                (
                    positions[change.token_id]
                    for positions in self.state.target_positions.values()
                    if change.token_id in positions
                ),
                None,
            )
            await markets.ensure(change.token_id, position=reference)
            book = self.state.books.get(change.token_id)
            if book is None or book.freshness is Freshness.STALE:
                if not await reconciler.refresh_book(change.token_id):
                    return
                book = self.state.books[change.token_id]
            price = book.mid or book.best_ask or book.best_bid
            if price is None or price <= ZERO:
                return
            raw = ZERO
            target_entries: list[tuple[Decimal, Decimal, Decimal]] = []
            for address, weight in self.settings.targets.items():
                position = self.state.target_positions.get(address, {}).get(change.token_id)
                target_size = position.size if position else ZERO
                capital = self.settings.target_capital(address)
                raw += target_size * (self.settings.own_capital_usd / capital) * weight
                target_entries.append((target_size, capital, weight))
            current = self.state.own_positions.get(change.token_id)
            current_size = current.size if current else ZERO
            side = Side.BUY if raw >= current_size else Side.SELL
            depth = book.depth_within(side, self.settings.slippage_tolerance_pct)
            sizer = PositionSizer(
                max_depth_fraction=self.settings.max_depth_fraction,
                max_market_exposure_pct=self.settings.max_market_exposure_pct,
            )
            result = sizer.size_aggregate(
                SizingInput(
                    target_size=target_size,
                    target_capital=capital,
                    own_capital=self.settings.own_capital_usd,
                    market_price=price,
                    depth_shares=depth,
                    target_weight=weight,
                )
                for target_size, capital, weight in target_entries
            )
            await events.emit(
                "strategy",
                "sizing_decision",
                {
                    "token_id": change.token_id,
                    "desired_size": str(result.desired_size),
                    "raw_mirror": str(result.raw_mirror),
                    "liquidity_cap": str(result.liquidity_cap),
                    "risk_cap_shares": str(result.risk_cap_shares),
                    "limiting_factor": result.limiting_factor,
                },
            )
            await self.on_signal(
                SignalEvent(
                    token_id=change.token_id,
                    desired_size=result.desired_size,
                    price_hint=change.price_hint,
                    reason=f"target change {change.address} delta={change.size_delta}",
                )
            )
