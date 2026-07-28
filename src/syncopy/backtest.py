from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from syncopy.domain import ZERO, PriceLevel, Side, SizingInput
from syncopy.state import OrderBook
from syncopy.strategy import PositionSizer, marketable_limit


@dataclass(slots=True)
class SimPosition:
    size: Decimal = ZERO
    avg_price: Decimal = ZERO


@dataclass(slots=True)
class SimTrade:
    ts: str
    token_id: str
    side: str
    size: str
    price: str
    fee: str
    desired: str


@dataclass(slots=True)
class BacktestReport:
    initial_cash: Decimal
    ending_equity: Decimal
    pnl: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees: Decimal
    trades: list[SimTrade]
    skipped: int
    max_exposure: Decimal

    def to_json(self) -> dict[str, Any]:
        return {
            "initial_cash": str(self.initial_cash),
            "ending_equity": str(self.ending_equity),
            "pnl": str(self.pnl),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "fees": str(self.fees),
            "trade_count": len(self.trades),
            "skipped": self.skipped,
            "max_exposure": str(self.max_exposure),
            "trades": [asdict(trade) for trade in self.trades],
        }


class ReplayEngine:
    """Replays recorded events through the live PositionSizer and book fill model."""

    def __init__(
        self,
        *,
        own_capital: Decimal,
        target_capitals: dict[str, Decimal],
        target_weights: dict[str, Decimal],
        default_target_capital: Decimal,
        max_depth_fraction: Decimal,
        max_market_exposure_pct: Decimal,
        slippage_tolerance_pct: Decimal,
        fee_bps: Decimal = Decimal("0"),
        default_tick_size: Decimal = Decimal("0.01"),
        default_min_size: Decimal = Decimal("5"),
    ) -> None:
        self.initial_cash = own_capital
        self.cash = own_capital
        self.target_capitals = target_capitals
        self.target_weights = target_weights
        self.default_target_capital = default_target_capital
        self.slippage_tolerance_pct = slippage_tolerance_pct
        self.fee_bps = fee_bps
        self.default_tick_size = default_tick_size
        self.default_min_size = default_min_size
        self.sizer = PositionSizer(
            max_depth_fraction=max_depth_fraction,
            max_market_exposure_pct=max_market_exposure_pct,
        )
        self.books: dict[str, OrderBook] = {}
        self.targets: dict[str, dict[str, Decimal]] = {}
        self.positions: dict[str, SimPosition] = {}
        self.realized = ZERO
        self.fees = ZERO
        self.trades: list[SimTrade] = []
        self.skipped = 0
        self.max_exposure = ZERO

    def replay(self, events: Iterable[dict[str, Any]]) -> BacktestReport:
        for event in events:
            self.apply(event)
        unrealized = ZERO
        marked_value = ZERO
        for token_id, position in self.positions.items():
            book = self.books.get(token_id)
            mark = book.mid if book and book.mid is not None else position.avg_price
            marked_value += position.size * mark
            unrealized += position.size * (mark - position.avg_price)
        equity = self.cash + marked_value
        return BacktestReport(
            initial_cash=self.initial_cash,
            ending_equity=equity,
            pnl=equity - self.initial_cash,
            realized_pnl=self.realized,
            unrealized_pnl=unrealized,
            fees=self.fees,
            trades=self.trades,
            skipped=self.skipped,
            max_exposure=self.max_exposure,
        )

    def apply(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or event.get("event_type") or "")
        payload = event.get("payload", event)
        if not isinstance(payload, dict):
            return
        if event_type in {"book", "book_snapshot"}:
            token_id = str(payload.get("asset_id") or "")
            if not token_id:
                return
            book = self.books.setdefault(token_id, OrderBook(token_id))
            book.replace(
                [
                    PriceLevel(Decimal(str(item["price"])), Decimal(str(item["size"])))
                    for item in payload.get("bids", [])
                ],
                [
                    PriceLevel(Decimal(str(item["price"])), Decimal(str(item["size"])))
                    for item in payload.get("asks", [])
                ],
            )
        elif event_type == "price_change":
            for change in payload.get("price_changes", []):
                token_id = str(change["asset_id"])
                self.books.setdefault(token_id, OrderBook(token_id)).update_level(
                    Side(str(change["side"]).upper()),
                    Decimal(str(change["price"])),
                    Decimal(str(change["size"])),
                )
        elif event_type == "target_position_change":
            self._target_change(event, payload)

    def _target_change(self, event: dict[str, Any], payload: dict[str, Any]) -> None:
        address = str(payload["address"]).lower()
        token_id = str(payload["token_id"])
        after = Decimal(str(payload["after_size"]))
        self.targets.setdefault(address, {})[token_id] = after
        book = self.books.get(token_id)
        if book is None or book.mid is None:
            self.skipped += 1
            return
        raw = sum(
            (
                positions.get(token_id, ZERO)
                * (
                    self.initial_cash
                    / self.target_capitals.get(address, self.default_target_capital)
                )
                * self.target_weights.get(address, Decimal("1"))
                for address, positions in self.targets.items()
            ),
            ZERO,
        )
        current = self.positions.get(token_id, SimPosition()).size
        side = Side.BUY if raw >= current else Side.SELL
        depth = book.depth_within(side, self.slippage_tolerance_pct)
        inputs = [
            SizingInput(
                target_size=positions.get(token_id, ZERO),
                target_capital=self.target_capitals.get(address, self.default_target_capital),
                own_capital=self.initial_cash,
                market_price=book.mid,
                depth_shares=depth,
                target_weight=self.target_weights.get(address, Decimal("1")),
            )
            for address, positions in self.targets.items()
        ]
        desired = self.sizer.size_aggregate(inputs).desired_size
        delta = desired - current
        if abs(delta) < self.default_min_size:
            self.skipped += 1
            return
        side = Side.BUY if delta > ZERO else Side.SELL
        best = book.best_ask if side is Side.BUY else book.best_bid
        if best is None:
            self.skipped += 1
            return
        price = marketable_limit(
            side=side,
            best_price=best,
            tick_size=self.default_tick_size,
            buffer_ticks=1,
        )
        filled, average = book.consume_fill(side, abs(delta), price)
        if filled <= ZERO or average is None:
            self.skipped += 1
            return
        fee = filled * average * self.fee_bps / Decimal("10000")
        self.fees += fee
        position = self.positions.setdefault(token_id, SimPosition())
        if side is Side.BUY:
            new_size = position.size + filled
            position.avg_price = (position.size * position.avg_price + filled * average) / new_size
            position.size = new_size
            self.cash -= filled * average + fee
        else:
            filled = min(filled, position.size)
            self.realized += filled * (average - position.avg_price) - fee
            position.size -= filled
            self.cash += filled * average - fee
            if position.size <= ZERO:
                self.positions.pop(token_id, None)
        timestamp = str(event.get("ts") or payload.get("observed_at") or "")
        self.trades.append(
            SimTrade(
                ts=timestamp,
                token_id=token_id,
                side=side.value,
                size=str(filled),
                price=str(average),
                fee=str(fee),
                desired=str(desired),
            )
        )
        exposure = ZERO
        for token, open_position in self.positions.items():
            book = self.books.get(token)
            mark = book.mid if book is not None else None
            exposure += open_position.size * (mark if mark is not None else open_position.avg_price)
        self.max_exposure = max(self.max_exposure, exposure)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected object on line {line_number}")
            result.append(payload)
    return result
