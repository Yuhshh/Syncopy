from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from decimal import Decimal

import structlog

from syncopy.clients import DataApi
from syncopy.db import Repository
from syncopy.domain import ZERO, TargetPositionChange, utcnow
from syncopy.events import EventWriter
from syncopy.state import RuntimeState

log = structlog.get_logger()
ChangeCallback = Callable[[TargetPositionChange], Awaitable[None]]
AssetCallback = Callable[[set[str]], None]


class TargetTracker:
    def __init__(
        self,
        data_api: DataApi,
        repository: Repository,
        events: EventWriter,
        state: RuntimeState,
        targets: dict[str, Decimal],
        target_capitals: dict[str, Decimal],
        *,
        default_capital: Decimal,
        interval_s: float,
        on_change: ChangeCallback,
        on_assets: AssetCallback | None = None,
    ) -> None:
        self.data_api = data_api
        self.repository = repository
        self.events = events
        self.state = state
        self.targets = {address.lower(): weight for address, weight in targets.items()}
        self.target_capitals = target_capitals
        self.default_capital = default_capital
        self.interval_s = interval_s
        self.on_change = on_change
        self.on_assets = on_assets
        self._stopping = asyncio.Event()

    async def initialize(self) -> None:
        for address, weight in self.targets.items():
            await self.repository.upsert_target(
                address,
                self.target_capitals.get(address, self.default_capital),
                weight,
            )

    async def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        await self.initialize()
        while not self._stopping.is_set():
            started = asyncio.get_running_loop().time()
            results = await asyncio.gather(
                *(self.poll_target(address) for address in self.targets),
                return_exceptions=True,
            )
            for address, result in zip(self.targets, results, strict=True):
                if isinstance(result, BaseException):
                    self.state.health.last_error = f"target {address}: {result}"
                    log.error("target_poll_failed", address=address, error=str(result))
            self.state.health.last_target_poll = utcnow()
            elapsed = asyncio.get_running_loop().time() - started
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=max(0.05, self.interval_s - elapsed)
                )

    async def poll_target(self, address: str) -> list[TargetPositionChange]:
        positions, trades = await asyncio.gather(
            self.data_api.positions(address), self.data_api.trades(address, limit=100)
        )
        snapshot_at = utcnow()
        incoming = {position.token_id: position for position in positions if position.size > ZERO}
        before = self.state.target_positions.get(address, {})
        trade_hints: dict[str, Decimal] = {}
        for trade in sorted(trades, key=lambda item: item.timestamp):
            trade_hints[trade.asset] = trade.price
        changes: list[TargetPositionChange] = []
        for token_id in sorted(set(before) | set(incoming)):
            old = before.get(token_id)
            new = incoming.get(token_id)
            old_size = old.size if old else ZERO
            new_size = new.size if new else ZERO
            if old_size == new_size:
                continue
            reference = new or old
            assert reference is not None
            changes.append(
                TargetPositionChange(
                    address=address,
                    token_id=token_id,
                    condition_id=reference.condition_id,
                    before_size=old_size,
                    after_size=new_size,
                    size_delta=new_size - old_size,
                    price_hint=trade_hints.get(token_id)
                    or (new.current_price if new is not None else reference.current_price),
                    observed_at=snapshot_at,
                )
            )
        await self.state.set_target_positions(address, incoming)
        await self.repository.save_target_snapshot(address, list(incoming.values()), snapshot_at)
        await self.events.emit(
            "data_api",
            "target_snapshot",
            {
                "address": address,
                "snapshot_at": snapshot_at.isoformat(),
                "positions": [
                    {
                        "token_id": position.token_id,
                        "condition_id": position.condition_id,
                        "size": str(position.size),
                        "avg_price": str(position.avg_price),
                        "current_price": (
                            str(position.current_price)
                            if position.current_price is not None
                            else None
                        ),
                    }
                    for position in incoming.values()
                ],
            },
        )
        if self.on_assets is not None:
            self.on_assets(set(incoming))
        for change in changes:
            await self.events.emit(
                "target_tracker",
                "target_position_change",
                {
                    "address": change.address,
                    "token_id": change.token_id,
                    "condition_id": change.condition_id,
                    "before_size": str(change.before_size),
                    "after_size": str(change.after_size),
                    "size_delta": str(change.size_delta),
                    "price_hint": (
                        str(change.price_hint) if change.price_hint is not None else None
                    ),
                    "observed_at": change.observed_at.isoformat(),
                },
            )
            await self.on_change(change)
        return changes
