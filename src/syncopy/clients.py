from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime
from decimal import Decimal
from typing import Any

import aiohttp
import structlog

from syncopy.domain import ZERO, MarketInfo, Position, PriceLevel, utcnow
from syncopy.messages import DataPosition, DataTrade, RestBook

log = structlog.get_logger()


class ApiError(RuntimeError):
    pass


class HttpApi:
    def __init__(
        self,
        base_url: str,
        session: aiohttp.ClientSession,
        *,
        attempts: int = 5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.attempts = attempts

    async def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        for attempt in range(self.attempts):
            try:
                async with self.session.get(f"{self.base_url}{path}", params=params) as response:
                    if response.status == 429 or response.status >= 500:
                        retry_after = response.headers.get("Retry-After")
                        delay = (
                            float(retry_after)
                            if retry_after
                            else min(8.0, 0.25 * 2**attempt) + random.random() * 0.2
                        )
                        if attempt + 1 == self.attempts:
                            body = await response.text()
                            raise ApiError(f"GET {path} failed {response.status}: {body[:500]}")
                        await asyncio.sleep(delay)
                        continue
                    if response.status >= 400:
                        body = await response.text()
                        raise ApiError(f"GET {path} failed {response.status}: {body[:500]}")
                    return await response.json(content_type=None)
            except (aiohttp.ClientError, TimeoutError) as error:
                if attempt + 1 == self.attempts:
                    raise ApiError(f"GET {path} failed after retries: {error}") from error
                await asyncio.sleep(min(8.0, 0.25 * 2**attempt) + random.random() * 0.2)
        raise AssertionError("unreachable")


class DataApi(HttpApi):
    async def positions(self, address: str) -> list[Position]:
        offset = 0
        positions: list[Position] = []
        observed_at = utcnow()
        while True:
            payload = await self.get_json(
                "/positions",
                params={
                    "user": address,
                    "sizeThreshold": 0,
                    "limit": 500,
                    "offset": offset,
                },
            )
            if not isinstance(payload, list):
                raise ApiError(f"Unexpected positions payload: {type(payload).__name__}")
            page = [DataPosition.model_validate(item) for item in payload]
            positions.extend(
                Position(
                    token_id=item.asset,
                    condition_id=item.condition_id,
                    size=item.size,
                    avg_price=item.avg_price,
                    current_price=item.current_price,
                    title=item.title,
                    end_date=item.end_date,
                    observed_at=observed_at,
                )
                for item in page
            )
            if len(page) < 500:
                return positions
            offset += len(page)

    async def trades(self, address: str | None, *, limit: int = 100) -> list[DataTrade]:
        params: dict[str, Any] = {
            "limit": min(limit, 10_000),
            "takerOnly": "false",
        }
        if address:
            params["user"] = address
        payload = await self.get_json("/trades", params=params)
        if not isinstance(payload, list):
            raise ApiError(f"Unexpected trades payload: {type(payload).__name__}")
        return [DataTrade.model_validate(item) for item in payload]


class ClobApi(HttpApi):
    async def book_raw(self, token_id: str) -> dict[str, Any]:
        payload = await self.get_json("/book", params={"token_id": token_id})
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected book payload: {type(payload).__name__}")
        return payload

    async def book(self, token_id: str) -> RestBook:
        return RestBook.model_validate(await self.book_raw(token_id))

    async def market_info(
        self,
        token_id: str,
        *,
        condition_id: str = "",
        question: str = "",
        end_date: datetime | None = None,
    ) -> MarketInfo:
        payload = await self.book_raw(token_id)
        return MarketInfo(
            token_id=token_id,
            condition_id=str(payload.get("market") or condition_id),
            question=question,
            tick_size=Decimal(str(payload.get("tick_size") or "0.01")),
            min_size=Decimal(str(payload.get("min_order_size") or "5")),
            active=True,
            end_date=end_date,
            neg_risk=bool(payload.get("neg_risk", False)),
        )


def _decode_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


class GammaApi(HttpApi):
    async def discover(self, *, limit: int = 100, offset: int = 0) -> list[MarketInfo]:
        payload = await self.get_json(
            "/markets",
            params={
                "active": "true",
                "closed": "false",
                "enable_order_book": "true",
                "limit": limit,
                "offset": offset,
                "order": "volume24hr",
                "ascending": "false",
            },
        )
        if not isinstance(payload, list):
            raise ApiError(f"Unexpected Gamma payload: {type(payload).__name__}")
        result: list[MarketInfo] = []
        for market in payload:
            tokens = _decode_json_list(market.get("clobTokenIds"))
            end_date = market.get("endDate")
            parsed_end = (
                datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                if isinstance(end_date, str) and end_date
                else None
            )
            for token_id in tokens:
                result.append(
                    MarketInfo(
                        token_id=str(token_id),
                        condition_id=str(market.get("conditionId") or ""),
                        question=str(market.get("question") or ""),
                        tick_size=Decimal(str(market.get("orderPriceMinTickSize") or "0.01")),
                        min_size=Decimal(str(market.get("orderMinSize") or "5")),
                        active=bool(market.get("active", True)) and not bool(market.get("closed")),
                        end_date=parsed_end,
                        neg_risk=bool(market.get("negRisk", False)),
                    )
                )
        return result


def rest_book_levels(book: RestBook) -> tuple[list[PriceLevel], list[PriceLevel]]:
    bids = [PriceLevel(level.price, level.size) for level in book.bids if level.size > ZERO]
    asks = [PriceLevel(level.price, level.size) for level in book.asks if level.size > ZERO]
    return bids, asks
