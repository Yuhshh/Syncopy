from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

ZERO = Decimal("0")


def utcnow() -> datetime:
    return datetime.now(UTC)


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    INTENT = "INTENT"
    SKIPPED = "SKIPPED"
    SUBMITTING = "SUBMITTING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    ERROR = "ERROR"


class Freshness(StrEnum):
    STALE = "STALE"
    FRESH = "FRESH"


@dataclass(frozen=True, slots=True)
class PriceLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class MarketInfo:
    token_id: str
    condition_id: str
    question: str
    tick_size: Decimal
    min_size: Decimal
    active: bool = True
    end_date: datetime | None = None
    neg_risk: bool = False


@dataclass(frozen=True, slots=True)
class Position:
    token_id: str
    size: Decimal
    avg_price: Decimal = ZERO
    condition_id: str = ""
    current_price: Decimal | None = None
    title: str = ""
    end_date: datetime | None = None
    observed_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class TargetPositionChange:
    address: str
    token_id: str
    condition_id: str
    before_size: Decimal
    after_size: Decimal
    size_delta: Decimal
    price_hint: Decimal | None
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class SizingInput:
    target_size: Decimal
    target_capital: Decimal
    own_capital: Decimal
    market_price: Decimal
    depth_shares: Decimal
    target_weight: Decimal = Decimal("1")


@dataclass(frozen=True, slots=True)
class SizingResult:
    desired_size: Decimal
    raw_mirror: Decimal
    liquidity_cap: Decimal
    risk_cap_shares: Decimal
    limiting_factor: str


@dataclass(frozen=True, slots=True)
class OrderIntent:
    client_id: str
    token_id: str
    side: Side
    size: Decimal
    price: Decimal
    desired_position: Decimal
    current_position: Decimal
    reason: str
    created_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    client_id: str
    size: Decimal
    price: Decimal
    fee: Decimal
    filled_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    client_id: str
    status: OrderStatus
    exchange_order_id: str | None = None
    filled_size: Decimal = ZERO
    average_price: Decimal | None = None
    message: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
