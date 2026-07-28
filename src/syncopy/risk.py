from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from syncopy.config import Settings
from syncopy.domain import ZERO, MarketInfo, Side
from syncopy.state import RuntimeState


@dataclass(frozen=True, slots=True)
class RiskDecision:
    allowed: bool
    size: Decimal
    reason: str


class RiskManager:
    def __init__(self, settings: Settings, state: RuntimeState) -> None:
        self.settings = settings
        self.state = state

    def static_check(self, market: MarketInfo) -> str | None:
        if self.settings.kill_switch:
            return "environment kill switch is enabled"
        if market.token_id in self.settings.blocked_token_ids:
            return "token is blocklisted"
        if not market.active:
            return "market is inactive"
        if market.end_date is not None:
            end = market.end_date
            if end.tzinfo is None:
                end = end.replace(tzinfo=UTC)
            seconds = (end - datetime.now(UTC)).total_seconds()
            if seconds <= float(self.settings.min_days_to_resolution * Decimal("86400")):
                return "market is too close to resolution"
        if self.state.health.trading_halted:
            return self.state.health.halt_reason or "trading is halted"
        return None

    def order_check(
        self,
        *,
        market: MarketInfo,
        side: Side,
        requested_size: Decimal,
        price: Decimal,
    ) -> RiskDecision:
        reason = self.static_check(market)
        if reason:
            return RiskDecision(False, ZERO, reason)
        max_order_shares = self.settings.max_order_size_usd / price
        size = min(requested_size, max_order_shares)
        current_total = ZERO
        for token_id, position in self.state.own_positions.items():
            book = self.state.books.get(token_id)
            mark = book.mid if book and book.mid is not None else position.avg_price
            current_total += position.size * mark
        total_cap = (
            self.settings.own_capital_usd * self.settings.max_total_exposure_pct / Decimal("100")
        )
        if side is Side.BUY:
            remaining_notional = max(ZERO, total_cap - current_total)
            size = min(size, remaining_notional / price)
        if size <= ZERO:
            return RiskDecision(False, ZERO, "portfolio exposure cap reached")
        return RiskDecision(True, size, "allowed")
