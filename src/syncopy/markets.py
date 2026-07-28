from __future__ import annotations

import asyncio

from syncopy.clients import ClobApi
from syncopy.db import Repository
from syncopy.domain import MarketInfo, Position


class MarketRegistry:
    def __init__(self, clob: ClobApi, repository: Repository) -> None:
        self.clob = clob
        self.repository = repository
        self.items: dict[str, MarketInfo] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def ensure(self, token_id: str, *, position: Position | None = None) -> MarketInfo:
        cached = self.items.get(token_id)
        if cached is not None:
            return cached
        lock = self._locks.setdefault(token_id, asyncio.Lock())
        async with lock:
            cached = self.items.get(token_id)
            if cached is not None:
                return cached
            market = await self.clob.market_info(
                token_id,
                condition_id=position.condition_id if position else "",
                question=position.title if position else "",
                end_date=position.end_date if position else None,
            )
            self.items[token_id] = market
            await self.repository.upsert_market(market)
            return market

    async def add(self, market: MarketInfo) -> None:
        self.items[market.token_id] = market
        await self.repository.upsert_market(market)
