from __future__ import annotations

from collections.abc import Iterable
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from syncopy.domain import ZERO, Side, SizingInput, SizingResult


class PositionSizer:
    def __init__(self, *, max_depth_fraction: Decimal, max_market_exposure_pct: Decimal) -> None:
        self.max_depth_fraction = max_depth_fraction
        self.max_market_exposure_pct = max_market_exposure_pct

    def size(self, item: SizingInput) -> SizingResult:
        return self.size_aggregate([item])

    def size_aggregate(self, items: Iterable[SizingInput]) -> SizingResult:
        entries = list(items)
        if not entries:
            return SizingResult(ZERO, ZERO, ZERO, ZERO, "no_targets")
        market_price = entries[0].market_price
        if market_price <= ZERO:
            return SizingResult(ZERO, ZERO, ZERO, ZERO, "invalid_price")
        raw = sum(
            (
                item.target_size * (item.own_capital / item.target_capital) * item.target_weight
                for item in entries
                if item.target_capital > ZERO
            ),
            ZERO,
        )
        depth = min(item.depth_shares for item in entries)
        liquidity_cap = depth * self.max_depth_fraction
        risk_cap = (
            entries[0].own_capital * self.max_market_exposure_pct / Decimal("100") / market_price
        )
        desired = max(ZERO, min(raw, liquidity_cap, risk_cap))
        caps = {
            "raw_mirror": raw,
            "liquidity": liquidity_cap,
            "market_risk": risk_cap,
        }
        limiting = min(caps, key=caps.__getitem__)
        return SizingResult(desired, raw, liquidity_cap, risk_cap, limiting)


def marketable_limit(
    *,
    side: Side,
    best_price: Decimal,
    tick_size: Decimal,
    buffer_ticks: int,
) -> Decimal:
    buffered = (
        best_price + tick_size * buffer_ticks
        if side is Side.BUY
        else best_price - tick_size * buffer_ticks
    )
    bounded = min(Decimal("1") - tick_size, max(tick_size, buffered))
    rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
    ticks = (bounded / tick_size).to_integral_value(rounding=rounding)
    return ticks * tick_size


def round_size_down(size: Decimal, precision: int = 4) -> Decimal:
    quantum = Decimal(1).scaleb(-precision)
    return size.quantize(quantum, rounding=ROUND_FLOOR)
