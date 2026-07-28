from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Iterable

import structlog
import websockets
from pydantic import ValidationError

from syncopy.clients import ClobApi, rest_book_levels
from syncopy.domain import PriceLevel, Side, utcnow
from syncopy.events import EventWriter
from syncopy.messages import WsBook, WsPriceChange
from syncopy.state import RuntimeState
from syncopy.tls import default_ssl_context

log = structlog.get_logger()
ReconnectCallback = Callable[[], Awaitable[None]]


class MarketIngestor:
    def __init__(
        self,
        ws_url: str,
        clob: ClobApi,
        state: RuntimeState,
        events: EventWriter,
        *,
        batch_size: int = 50,
        on_reconnect: ReconnectCallback | None = None,
    ) -> None:
        self.ws_url = ws_url
        self.clob = clob
        self.state = state
        self.events = events
        self.batch_size = batch_size
        self.on_reconnect = on_reconnect
        self._assets: set[str] = set()
        self._changed = asyncio.Event()
        self._stopping = asyncio.Event()
        self._ssl_context = default_ssl_context()

    def set_assets(self, token_ids: Iterable[str]) -> None:
        updated = {str(token_id) for token_id in token_ids if token_id}
        if updated != self._assets:
            self._assets = updated
            self._changed.set()

    def add_assets(self, token_ids: Iterable[str]) -> None:
        before = len(self._assets)
        self._assets.update(str(token_id) for token_id in token_ids if token_id)
        if len(self._assets) != before:
            self._changed.set()

    async def stop(self) -> None:
        self._stopping.set()
        self._changed.set()

    async def run(self) -> None:
        while not self._stopping.is_set():
            if not self._assets:
                self._changed.clear()
                await self._changed.wait()
                continue
            assets = sorted(self._assets)
            self._changed.clear()
            batches = [
                assets[index : index + self.batch_size]
                for index in range(0, len(assets), self.batch_size)
            ]
            tasks = [
                asyncio.create_task(self._connection_loop(batch), name=f"market-ws-{index}")
                for index, batch in enumerate(batches)
            ]
            changed = asyncio.create_task(self._changed.wait(), name="market-assets-changed")
            stopped = asyncio.create_task(self._stopping.wait(), name="market-ws-stop")
            done, _ = await asyncio.wait(
                [*tasks, changed, stopped], return_when=asyncio.FIRST_COMPLETED
            )
            if any(task in done and task.exception() for task in tasks):
                for task in tasks:
                    if task in done and task.exception():
                        log.error("market_ws_worker_failed", error=str(task.exception()))
            for task in tasks:
                if not task.done():
                    task.cancel()
            if not changed.done():
                changed.cancel()
            if not stopped.done():
                stopped.cancel()
            await asyncio.gather(*tasks, changed, stopped, return_exceptions=True)

    async def _connection_loop(self, assets: list[str]) -> None:
        backoff = 0.5
        first_connection = True
        while not self._stopping.is_set() and not self._changed.is_set():
            await self.state.mark_books_stale(assets)
            await self._refresh_books(assets)
            if not first_connection and self.on_reconnect is not None:
                await self.on_reconnect()
            first_connection = False
            try:
                async with websockets.connect(
                    self.ws_url,
                    ssl=self._ssl_context,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=4096,
                ) as websocket:
                    await websocket.send(json.dumps({"assets_ids": assets, "type": "market"}))
                    await self.events.emit("market_ws", "subscribed", {"assets_ids": assets})
                    backoff = 0.5
                    async for raw in websocket:
                        self.state.health.last_ws_message = utcnow()
                        await self._handle_raw(raw)
                        if self._changed.is_set() or self._stopping.is_set():
                            return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.state.health.last_error = f"market websocket: {error}"
                await self.events.emit(
                    "market_ws",
                    "disconnected",
                    {"assets_ids": assets, "error": str(error), "retry_in_s": backoff},
                )
                log.warning("market_ws_disconnected", error=str(error), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2)

    async def _refresh_books(self, assets: list[str]) -> None:
        semaphore = asyncio.Semaphore(10)

        async def refresh(token_id: str) -> None:
            async with semaphore:
                try:
                    book = await self.clob.book(token_id)
                    bids, asks = rest_book_levels(book)
                    await self.state.replace_book(token_id, bids, asks, sequence_hash=book.hash)
                    await self.events.emit(
                        "clob_rest",
                        "book_snapshot",
                        book.model_dump(mode="json", by_alias=True),
                    )
                except Exception as error:
                    log.warning("book_snapshot_failed", token_id=token_id, error=str(error))

        await asyncio.gather(*(refresh(asset) for asset in assets))

    async def _handle_raw(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode()
        if raw in {"PING", "PONG"}:
            return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await self.events.emit("market_ws", "invalid_json", {"raw": raw[:2000]})
            return
        messages = payload if isinstance(payload, list) else [payload]
        for message in messages:
            if not isinstance(message, dict):
                continue
            event_type = str(message.get("event_type") or "unknown")
            await self.events.emit("market_ws", event_type, message)
            try:
                if event_type == "book":
                    book_event = WsBook.model_validate(message)
                    await self.state.replace_book(
                        book_event.asset_id,
                        [PriceLevel(item.price, item.size) for item in book_event.bids],
                        [PriceLevel(item.price, item.size) for item in book_event.asks],
                        sequence_hash=book_event.hash,
                    )
                elif event_type == "price_change":
                    change_event = WsPriceChange.model_validate(message)
                    for change in change_event.price_changes:
                        await self.state.update_book_level(
                            change.asset_id,
                            Side(change.side.upper()),
                            change.price,
                            change.size,
                        )
            except (ValidationError, ValueError) as error:
                await self.events.emit(
                    "market_ws",
                    "validation_error",
                    {"event_type": event_type, "error": str(error), "payload": message},
                )
