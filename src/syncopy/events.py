from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import structlog

from syncopy.db import Repository
from syncopy.domain import utcnow

log = structlog.get_logger()
EventItem = tuple[datetime, str, str, dict[str, Any]]


class EventWriter:
    """Bounded, batched event writer. Backpressure prevents silent event loss."""

    def __init__(
        self,
        repository: Repository,
        *,
        batch_size: int = 200,
        flush_interval_s: float = 0.25,
        queue_size: int = 20_000,
    ) -> None:
        self.repository = repository
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.queue: asyncio.Queue[EventItem] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name="event-writer")

    async def emit(
        self,
        source: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        ts: datetime | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("Event writer is closed")
        await self.queue.put((ts or utcnow(), source, event_type, payload))

    async def run(self) -> None:
        batch: list[EventItem] = []
        while not self._closed or not self.queue.empty():
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval_s)
                batch.append(item)
                self.queue.task_done()
            except TimeoutError:
                pass
            if batch and (len(batch) >= self.batch_size or self.queue.empty()):
                try:
                    await self.repository.events(batch)
                except Exception:
                    log.exception("event_batch_write_failed", count=len(batch))
                    for item in reversed(batch):
                        try:
                            self.queue.put_nowait(item)
                        except asyncio.QueueFull:
                            log.critical("event_queue_full_during_retry")
                            break
                    await asyncio.sleep(1)
                batch = []

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            await self._task
