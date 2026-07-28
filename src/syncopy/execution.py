from __future__ import annotations

import asyncio
import contextlib
import hashlib
from collections.abc import Awaitable, Callable
from datetime import timedelta
from decimal import Decimal
from typing import Any

import structlog

from syncopy.config import Settings
from syncopy.db import Repository
from syncopy.domain import (
    ZERO,
    ExecutionResult,
    Fill,
    Freshness,
    OrderIntent,
    OrderStatus,
    Side,
    utcnow,
)
from syncopy.events import EventWriter
from syncopy.markets import MarketRegistry
from syncopy.risk import RiskManager
from syncopy.state import RuntimeState
from syncopy.strategy import marketable_limit, round_size_down

log = structlog.get_logger()


UserEventCallback = Callable[[dict[str, Any]], Awaitable[None]]


class PolymarketExecutor:
    """Thin adapter around the supported 2026 unified `polymarket-client` SDK."""

    def __init__(self, private_key: str, wallet: str) -> None:
        self.private_key = private_key
        self.wallet = wallet
        self.client: Any = None

    async def start(self) -> None:
        try:
            from polymarket import AsyncSecureClient
        except ImportError as error:
            raise RuntimeError(
                "Live mode requires the 'live' extra: pip install 'syncopy[live]'"
            ) from error
        self.client = await AsyncSecureClient.create(
            private_key=self.private_key, wallet=self.wallet
        )

    async def place(self, intent: OrderIntent) -> dict[str, Any]:
        if self.client is None:
            raise RuntimeError("Polymarket executor has not been started")
        response = await self.client.place_limit_order(
            token_id=intent.token_id,
            price=intent.price,
            size=intent.size,
            side=intent.side.value,
            post_only=False,
        )
        if hasattr(response, "model_dump"):
            return dict(response.model_dump(mode="json"))
        if isinstance(response, dict):
            return response
        return {"response": str(response)}

    async def cancel(self, exchange_order_id: str) -> dict[str, Any]:
        if self.client is None:
            raise RuntimeError("Polymarket executor has not been started")
        response = await self.client.cancel_order(order_id=exchange_order_id)
        return (
            dict(response.model_dump(mode="json"))
            if hasattr(response, "model_dump")
            else {"response": str(response)}
        )

    async def cancel_all(self) -> dict[str, Any]:
        if self.client is None:
            return {}
        response = await self.client.cancel_all()
        return (
            dict(response.model_dump(mode="json"))
            if hasattr(response, "model_dump")
            else {"response": str(response)}
        )

    async def collateral_balance(self) -> Decimal:
        if self.client is None:
            raise RuntimeError("Polymarket executor has not been started")
        balance = await self.client.get_balance_allowance(asset_type="COLLATERAL")
        return Decimal(balance.balance) / Decimal("1000000")

    async def user_stream(self, callback: UserEventCallback) -> None:
        if self.client is None:
            raise RuntimeError("Polymarket executor has not been started")
        from polymarket.streams import UserSpec

        handle = await self.client.subscribe(UserSpec())
        try:
            async for event in handle:
                await callback(dict(event.model_dump(mode="json")))
        finally:
            await handle.close()

    async def close(self) -> None:
        if self.client is not None:
            await self.client.close()
            self.client = None


class ExecutionEngine:
    def __init__(
        self,
        settings: Settings,
        state: RuntimeState,
        repository: Repository,
        events: EventWriter,
        markets: MarketRegistry,
        risk: RiskManager,
        live_executor: PolymarketExecutor | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.repository = repository
        self.events = events
        self.markets = markets
        self.risk = risk
        self.live_executor = live_executor
        self._stopping = asyncio.Event()
        self.paper_cash = settings.own_capital_usd
        self.paper_realized = ZERO

    async def start(self) -> None:
        if self.settings.dry_run:
            self.paper_cash += await self.repository.cumulative_cashflow()
        else:
            if self.live_executor is None:
                raise RuntimeError("Live executor is not configured")
            await self.live_executor.start()

    async def stop(self) -> None:
        self._stopping.set()
        if self.live_executor is not None:
            await self.live_executor.close()

    async def cancel_all_and_halt(self, reason: str) -> None:
        self.state.health.trading_halted = True
        self.state.health.halt_reason = reason
        raw: dict[str, Any] = {}
        if self.live_executor is not None and not self.settings.dry_run:
            raw = await self.live_executor.cancel_all()
        await self.events.emit("risk", "trading_halted", {"reason": reason, "response": raw})

    async def process(
        self,
        token_id: str,
        desired_position: Decimal,
        *,
        reason: str,
        price_hint: Decimal | None = None,
    ) -> ExecutionResult | None:
        book = self.state.books.get(token_id)
        if book is None or book.freshness is not Freshness.FRESH:
            await self._skip(token_id, "book state is stale")
            return None
        current = self.state.own_positions.get(token_id)
        current_size = current.size if current else ZERO
        delta = desired_position - current_size
        if delta == ZERO:
            return None
        side = Side.BUY if delta > ZERO else Side.SELL
        best = book.best_ask if side is Side.BUY else book.best_bid
        if best is None:
            await self._skip(token_id, f"no {'asks' if side is Side.BUY else 'bids'}")
            return None
        market = await self.markets.ensure(token_id)
        if abs(delta) < market.min_size:
            await self._skip(
                token_id,
                "PARTIAL_SKIP: position delta is below market minimum",
                delta=str(delta),
                min_size=str(market.min_size),
            )
            return None
        if price_hint is not None and price_hint > ZERO:
            move_pct = abs(best - price_hint) / price_hint * Decimal("100")
            if move_pct > self.settings.max_target_price_move_pct:
                await self._skip(
                    token_id,
                    "adverse-selection price move exceeded",
                    target_price=str(price_hint),
                    current_price=str(best),
                    move_pct=str(move_pct),
                )
                return None
        limit_price = marketable_limit(
            side=side,
            best_price=best,
            tick_size=market.tick_size,
            buffer_ticks=self.settings.price_buffer_ticks,
        )
        decision = self.risk.order_check(
            market=market,
            side=side,
            requested_size=abs(delta),
            price=limit_price,
        )
        size = round_size_down(decision.size)
        if not decision.allowed or size < market.min_size:
            await self._skip(
                token_id,
                decision.reason if not decision.allowed else "risk-clipped size below minimum",
                requested_size=str(abs(delta)),
                allowed_size=str(size),
            )
            return None
        intent = OrderIntent(
            client_id=self._client_id(token_id, side, current_size, desired_position, limit_price),
            token_id=token_id,
            side=side,
            size=size,
            price=limit_price,
            desired_position=desired_position,
            current_position=current_size,
            reason=reason,
        )
        if not await self.repository.reserve_intent(intent):
            await self.events.emit(
                "execution", "duplicate_intent_suppressed", {"client_id": intent.client_id}
            )
            return None
        await self.events.emit(
            "execution",
            "order_intent",
            {
                "client_id": intent.client_id,
                "token_id": token_id,
                "side": side.value,
                "size": str(size),
                "price": str(limit_price),
                "desired_position": str(desired_position),
                "current_position": str(current_size),
                "reason": reason,
            },
        )
        if self.settings.dry_run:
            return await self._paper_execute(intent)
        return await self._live_execute(intent)

    async def _paper_execute(self, intent: OrderIntent) -> ExecutionResult:
        book = self.state.books[intent.token_id]
        filled, average = book.consume_fill(intent.side, intent.size, intent.price)
        if filled <= ZERO or average is None:
            result = ExecutionResult(
                intent.client_id, OrderStatus.REJECTED, message="no liquidity at limit"
            )
            await self.repository.update_order(result)
            return result
        status = OrderStatus.FILLED if filled == intent.size else OrderStatus.PARTIALLY_FILLED
        result = ExecutionResult(
            client_id=intent.client_id,
            status=status,
            exchange_order_id=f"paper-{intent.client_id}",
            filled_size=filled,
            average_price=average,
        )
        await self.repository.update_order(result)
        fill = Fill(
            fill_id=f"paper-fill-{intent.client_id}",
            client_id=intent.client_id,
            size=filled,
            price=average,
            fee=ZERO,
        )
        await self.repository.save_fill(fill)
        before_position = self.state.own_positions.get(intent.token_id)
        before_avg = before_position.avg_price if before_position else ZERO
        notional = filled * average
        if intent.side is Side.BUY:
            self.paper_cash -= notional + fill.fee
        else:
            self.paper_cash += notional - fill.fee
            self.paper_realized += filled * (average - before_avg) - fill.fee
        signed_delta = filled if intent.side is Side.BUY else -filled
        await self.state.adjust_own_position(intent.token_id, signed_delta, average)
        await self.repository.replace_own_positions(list(self.state.own_positions.values()))
        await self.events.emit(
            "paper_execution",
            "fill",
            {
                "fill_id": fill.fill_id,
                "client_id": intent.client_id,
                "token_id": intent.token_id,
                "side": intent.side.value,
                "size": str(filled),
                "price": str(average),
                "status": status.value,
            },
        )
        return result

    async def user_event_loop(self) -> None:
        if self.settings.dry_run:
            await self._stopping.wait()
            return
        assert self.live_executor is not None
        await self.live_executor.user_stream(self._handle_user_event)

    async def _handle_user_event(self, event: dict[str, Any]) -> None:
        await self.events.emit("user_ws", str(event.get("type") or "unknown"), event)
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return
        if event.get("type") == "order":
            exchange_id = str(payload.get("id") or "")
            rows = await self.repository.orders_by_exchange_ids([exchange_id])
            if not rows:
                return
            raw_status = str(payload.get("status") or payload.get("order_event_type") or "")
            status = {
                "MATCHED": OrderStatus.FILLED,
                "CANCELED": OrderStatus.CANCELLED,
                "CANCELLATION": OrderStatus.CANCELLED,
                "UNMATCHED": OrderStatus.REJECTED,
                "LIVE": OrderStatus.OPEN,
                "PLACEMENT": OrderStatus.OPEN,
                "UPDATE": OrderStatus.PARTIALLY_FILLED,
                "DELAYED": OrderStatus.OPEN,
            }.get(raw_status, OrderStatus.OPEN)
            for row in rows:
                await self.repository.update_order(
                    ExecutionResult(
                        client_id=row.client_id,
                        status=status,
                        exchange_order_id=exchange_id,
                        raw=event,
                    )
                )
        elif event.get("type") == "trade":
            await self._handle_user_trade(payload)

    async def _handle_user_trade(self, payload: dict[str, Any]) -> None:
        trade_id = str(payload.get("id") or "")
        candidates: dict[str, tuple[Side, Decimal, Decimal]] = {}
        taker_order_id = str(payload.get("taker_order_id") or "")
        if taker_order_id:
            candidates[taker_order_id] = (
                Side(str(payload["side"]).upper()),
                Decimal(str(payload["size"])),
                Decimal(str(payload["price"])),
            )
        for maker in payload.get("maker_orders") or []:
            if isinstance(maker, dict):
                candidates[str(maker.get("order_id") or "")] = (
                    Side(str(maker["side"]).upper()),
                    Decimal(str(maker["matched_amount"])),
                    Decimal(str(maker["price"])),
                )
        rows = await self.repository.orders_by_exchange_ids(list(candidates))
        for row in rows:
            if row.exchange_order_id is None:
                continue
            side, size, price = candidates[row.exchange_order_id]
            fee_rate = Decimal(str(payload.get("fee_rate_bps") or "0"))
            fee = size * price * fee_rate / Decimal("10000")
            fill = Fill(
                fill_id=f"{trade_id}:{row.exchange_order_id}",
                client_id=row.client_id,
                size=size,
                price=price,
                fee=fee,
            )
            if not await self.repository.save_fill(fill):
                continue
            signed_delta = size if side is Side.BUY else -size
            await self.state.adjust_own_position(row.token_id, signed_delta, price)
            await self.repository.replace_own_positions(list(self.state.own_positions.values()))
            await self.repository.update_order(
                ExecutionResult(
                    client_id=row.client_id,
                    status=OrderStatus.PARTIALLY_FILLED,
                    exchange_order_id=row.exchange_order_id,
                    filled_size=size,
                    average_price=price,
                    raw=payload,
                )
            )

    async def pnl_loop(self) -> None:
        while not self._stopping.is_set():
            unrealized = ZERO
            marked_value = ZERO
            for token_id, position in self.state.own_positions.items():
                book = self.state.books.get(token_id)
                mark = book.mid if book is not None else None
                resolved_mark = mark if mark is not None else position.avg_price
                marked_value += position.size * resolved_mark
                unrealized += position.size * (resolved_mark - position.avg_price)
            if self.settings.dry_run:
                equity = self.paper_cash + marked_value
                realized = self.paper_realized
            else:
                assert self.live_executor is not None
                try:
                    cash = await self.live_executor.collateral_balance()
                except Exception as error:
                    log.warning("collateral_balance_failed", error=str(error))
                    cash = self.settings.own_capital_usd
                equity = cash + marked_value
                realized = ZERO
            await self.repository.save_pnl_snapshot(
                equity=equity, realized=realized, unrealized=unrealized
            )
            await self.events.emit(
                "pnl",
                "snapshot",
                {
                    "equity": str(equity),
                    "realized": str(realized),
                    "unrealized": str(unrealized),
                },
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self.settings.pnl_snapshot_interval_s,
                )

    async def _live_execute(self, intent: OrderIntent) -> ExecutionResult:
        assert self.live_executor is not None
        submitting = ExecutionResult(intent.client_id, OrderStatus.SUBMITTING)
        await self.repository.update_order(submitting)
        try:
            raw = await self.live_executor.place(intent)
            exchange_id = (
                raw.get("order_id") or raw.get("orderId") or raw.get("id") or raw.get("orderID")
            )
            status = (
                OrderStatus.FILLED
                if raw.get("status") == "matched"
                else OrderStatus.OPEN
                if exchange_id
                else OrderStatus.REJECTED
            )
            result = ExecutionResult(
                intent.client_id,
                status,
                exchange_order_id=str(exchange_id) if exchange_id else None,
                message=(
                    ""
                    if exchange_id
                    else str(raw.get("message") or raw.get("error") or "exchange rejected order")
                ),
                raw=raw,
            )
        except Exception as error:
            result = ExecutionResult(
                intent.client_id,
                OrderStatus.ERROR,
                message=str(error),
            )
            log.exception("live_order_failed", client_id=intent.client_id)
        await self.repository.update_order(result)
        await self.events.emit(
            "execution",
            "order_result",
            {
                "client_id": intent.client_id,
                "status": result.status.value,
                "exchange_order_id": result.exchange_order_id,
                "message": result.message,
                "raw": result.raw,
            },
        )
        return result

    async def ttl_loop(self) -> None:
        while not self._stopping.is_set():
            cutoff = utcnow() - timedelta(seconds=self.settings.order_ttl_s)
            for row in await self.repository.open_orders_older_than(cutoff):
                if not row.exchange_order_id or self.settings.dry_run:
                    continue
                assert self.live_executor is not None
                try:
                    raw = await self.live_executor.cancel(row.exchange_order_id)
                    await self.repository.update_order(
                        ExecutionResult(
                            row.client_id,
                            OrderStatus.CANCELLED,
                            exchange_order_id=row.exchange_order_id,
                            raw=raw,
                        )
                    )
                except Exception as error:
                    log.exception("cancel_failed", client_id=row.client_id, error=str(error))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=1)

    async def _skip(self, token_id: str, reason: str, **details: str) -> None:
        await self.events.emit(
            "execution", "order_skipped", {"token_id": token_id, "reason": reason, **details}
        )

    @staticmethod
    def _client_id(
        token_id: str,
        side: Side,
        current: Decimal,
        desired: Decimal,
        price: Decimal,
    ) -> str:
        material = f"v1|{token_id}|{side.value}|{current}|{desired}|{price}"
        return hashlib.sha256(material.encode()).hexdigest()
