from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterable
from decimal import Decimal

import structlog

from syncopy.clients import ClobApi, DataApi, rest_book_levels
from syncopy.db import Repository
from syncopy.domain import ZERO, Position, TargetPositionChange, utcnow
from syncopy.events import EventWriter
from syncopy.state import RuntimeState

log = structlog.get_logger()
ChangeCallback = Callable[[TargetPositionChange], Awaitable[None]]
HaltCallback = Callable[[str], Awaitable[None]]


def _position_json(positions: dict[str, Position]) -> dict[str, dict[str, str | None]]:
    return {
        token_id: {
            "size": str(position.size),
            "avg_price": str(position.avg_price),
            "current_price": (
                str(position.current_price) if position.current_price is not None else None
            ),
        }
        for token_id, position in sorted(positions.items())
    }


class Reconciler:
    def __init__(
        self,
        clob: ClobApi,
        data_api: DataApi,
        repository: Repository,
        events: EventWriter,
        state: RuntimeState,
        *,
        own_wallet: str | None,
        targets: Iterable[str],
        interval_s: float,
        daily_loss_halt_usd: Decimal,
        on_target_change: ChangeCallback | None = None,
        on_halt: HaltCallback | None = None,
    ) -> None:
        self.clob = clob
        self.data_api = data_api
        self.repository = repository
        self.events = events
        self.state = state
        self.own_wallet = own_wallet
        self.targets = [address.lower() for address in targets]
        self.interval_s = interval_s
        self.daily_loss_halt_usd = daily_loss_halt_usd
        self.on_target_change = on_target_change
        self.on_halt = on_halt
        self._lock = asyncio.Lock()
        self._stopping = asyncio.Event()

    async def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.reconcile_all()
            except Exception as error:
                self.state.health.last_error = f"reconciliation: {error}"
                log.exception("reconciliation_failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=self.interval_s)

    async def reconcile_all(self) -> None:
        if self._lock.locked():
            return
        async with self._lock:
            token_ids = set(self.state.books)
            await asyncio.gather(
                self.reconcile_books(token_ids),
                self.reconcile_own(),
                *(self.reconcile_target(address) for address in self.targets),
            )
            daily = await self.repository.daily_pnl()
            if (
                daily <= -self.daily_loss_halt_usd
                and not self.state.health.trading_halted
                and self.on_halt is not None
            ):
                await self.on_halt(f"daily loss threshold breached: {daily}")
            self.state.health.last_reconciliation = utcnow()

    async def refresh_book(self, token_id: str) -> bool:
        await self.state.mark_books_stale([token_id])
        try:
            book = await self.clob.book(token_id)
        except Exception as error:
            await self.events.emit(
                "reconciler",
                "book_snapshot_failed",
                {"token_id": token_id, "error": str(error)},
            )
            return False
        bids, asks = rest_book_levels(book)
        before = self.state.books.get(token_id)
        before_json = (
            {
                "best_bid": str(before.best_bid) if before and before.best_bid else None,
                "best_ask": str(before.best_ask) if before and before.best_ask else None,
                "hash": before.sequence_hash if before else None,
            }
            if before
            else {}
        )
        await self.state.replace_book(token_id, bids, asks, sequence_hash=book.hash)
        after = self.state.books[token_id]
        after_json = {
            "best_bid": str(after.best_bid) if after.best_bid else None,
            "best_ask": str(after.best_ask) if after.best_ask else None,
            "hash": after.sequence_hash,
        }
        if before_json != after_json:
            await self.repository.reconciliation(
                f"book:{token_id}", before_json, after_json, "REST snapshot replaced local book"
            )
        return True

    async def reconcile_books(self, token_ids: Iterable[str]) -> None:
        semaphore = asyncio.Semaphore(10)

        async def one(token_id: str) -> None:
            async with semaphore:
                await self.refresh_book(token_id)

        await asyncio.gather(*(one(token_id) for token_id in token_ids))

    async def reconcile_own(self) -> None:
        if not self.own_wallet:
            return
        authoritative = {
            item.token_id: item
            for item in await self.data_api.positions(self.own_wallet)
            if item.size > ZERO
        }
        before = dict(self.state.own_positions)
        if _position_json(before) != _position_json(authoritative):
            await self.repository.reconciliation(
                "own_positions",
                _position_json(before),
                _position_json(authoritative),
                "Data API authoritative positions replaced local state",
            )
            await self.state.set_own_positions(authoritative)
            await self.repository.replace_own_positions(list(authoritative.values()))

    async def reconcile_target(self, address: str) -> None:
        authoritative = {
            item.token_id: item
            for item in await self.data_api.positions(address)
            if item.size > ZERO
        }
        before = dict(self.state.target_positions.get(address, {}))
        if _position_json(before) == _position_json(authoritative):
            return
        await self.repository.reconciliation(
            f"target:{address}",
            _position_json(before),
            _position_json(authoritative),
            "Data API authoritative positions replaced tracker state",
        )
        await self.state.set_target_positions(address, authoritative)
        snapshot_at = utcnow()
        await self.repository.save_target_snapshot(
            address, list(authoritative.values()), snapshot_at
        )
        if self.on_target_change is None:
            return
        for token_id in sorted(set(before) | set(authoritative)):
            old = before.get(token_id)
            new = authoritative.get(token_id)
            old_size = old.size if old else ZERO
            new_size = new.size if new else ZERO
            if old_size == new_size:
                continue
            reference = new or old
            assert reference is not None
            await self.on_target_change(
                TargetPositionChange(
                    address=address,
                    token_id=token_id,
                    condition_id=reference.condition_id,
                    before_size=old_size,
                    after_size=new_size,
                    size_delta=new_size - old_size,
                    price_hint=reference.current_price,
                    observed_at=snapshot_at,
                )
            )
