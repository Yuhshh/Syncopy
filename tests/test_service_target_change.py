"""Characterization tests for SyncopyService._on_target_change and
Reconciler.reconcile_target.  No network, no database.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from syncopy.config import Settings
from syncopy.domain import ZERO, Position, PriceLevel, TargetPositionChange, utcnow
from syncopy.reconcile import Reconciler
from syncopy.service import SyncopyService
from syncopy.state import OrderBook, RuntimeState

# ── shared constants ─────────────────────────────────────────────────────────

ADDR1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOKEN = "token-0001"


# ── helpers ──────────────────────────────────────────────────────────────────


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = dict(
        _env_file=None,
        targets={ADDR1: Decimal("1")},
        own_capital_usd=Decimal("1000"),
        default_target_capital_usd=Decimal("10000"),
        max_depth_fraction=Decimal("0.10"),
        max_market_exposure_pct=Decimal("100"),
        slippage_tolerance_pct=Decimal("2"),
        dry_run=True,
        auto_create_schema=False,
    )
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


def _make_service(settings: Settings) -> SyncopyService:
    """Create a SyncopyService with all I/O dependencies replaced by mocks.

    Only the RuntimeState and Settings are real; everything that touches the
    network or database is stubbed out.
    """
    svc = SyncopyService(settings)
    svc.ingestor = MagicMock()
    svc.reconciler = AsyncMock()
    svc.markets = AsyncMock()
    svc.events = AsyncMock()
    svc.execution = AsyncMock()
    return svc


def _fresh_book(
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
) -> OrderBook:
    book = OrderBook(TOKEN)
    book.replace(
        bids=[PriceLevel(Decimal(p), Decimal(s)) for p, s in bids],
        asks=[PriceLevel(Decimal(p), Decimal(s)) for p, s in asks],
    )
    return book


def _change(
    *,
    address: str = ADDR1,
    before: str = "0",
    after: str,
    price_hint: str | None = None,
) -> TargetPositionChange:
    b = Decimal(before)
    a = Decimal(after)
    return TargetPositionChange(
        address=address,
        token_id=TOKEN,
        condition_id="cond-1",
        before_size=b,
        after_size=a,
        size_delta=a - b,
        price_hint=Decimal(price_hint) if price_hint else None,
        observed_at=utcnow(),
    )


# ── _on_target_change tests ──────────────────────────────────────────────────


async def test_single_target_passes_raw_mirror_to_execution() -> None:
    """Raw mirror = target_size × (own_capital / target_capital) × weight.

    With target_capital=10000, own_capital=1000, target_size=50, weight=1:
      raw = 50 × (1000/10000) × 1 = 5
    Depth (5000) and risk cap (>>5) are non-binding, so desired == 5.
    """
    settings = _settings()
    svc = _make_service(settings)

    svc.state.books[TOKEN] = _fresh_book(
        bids=[("0.49", "100")],
        asks=[("0.50", "5000")],
    )
    svc.state.target_positions[ADDR1] = {
        TOKEN: Position(token_id=TOKEN, size=Decimal("50"), condition_id="cond-1")
    }

    await svc._on_target_change(_change(after="50"))

    svc.execution.process.assert_called_once_with(
        TOKEN,
        Decimal("5"),
        reason=f"target change {ADDR1} delta=50",
        price_hint=None,
    )


async def test_multiple_weighted_targets_aggregate_to_sum() -> None:
    """Two targets with different weights are summed proportionally.

    ADDR1: 20 shares × weight 0.6 × (1000/1000) = 12
    ADDR2: 30 shares × weight 0.4 × (1000/1000) = 12
    Aggregate raw = 24.  Depth and risk caps are large → desired == 24.
    """
    settings = _settings(
        targets={ADDR1: Decimal("0.6"), ADDR2: Decimal("0.4")},
        default_target_capital_usd=Decimal("1000"),
        max_depth_fraction=Decimal("1"),
        max_market_exposure_pct=Decimal("100"),
    )
    svc = _make_service(settings)

    svc.state.books[TOKEN] = _fresh_book(
        bids=[("0.49", "1000")],
        asks=[("0.50", "10000")],
    )
    svc.state.target_positions[ADDR1] = {
        TOKEN: Position(token_id=TOKEN, size=Decimal("20"), condition_id="cond-1")
    }
    svc.state.target_positions[ADDR2] = {
        TOKEN: Position(token_id=TOKEN, size=Decimal("30"), condition_id="cond-1")
    }

    await svc._on_target_change(_change(after="20"))

    svc.execution.process.assert_called_once_with(
        TOKEN,
        Decimal("24"),
        reason=f"target change {ADDR1} delta=20",
        price_hint=None,
    )


async def test_liquidity_cap_limits_desired_size() -> None:
    """depth × max_depth_fraction is binding when liquidity is shallow.

    raw = 100 × (1000/1000) × 1 = 100
    depth = 50 (only 50 shares in the book within slippage band)
    liquidity_cap = 50 × 0.10 = 5
    risk_cap = 1000 × 100/100 / mid >> 5
    → desired = 5, limiting_factor = 'liquidity'
    """
    settings = _settings(
        max_depth_fraction=Decimal("0.10"),
        max_market_exposure_pct=Decimal("100"),
        default_target_capital_usd=Decimal("1000"),
    )
    svc = _make_service(settings)

    # Only 50 shares available at the ask
    svc.state.books[TOKEN] = _fresh_book(
        bids=[("0.48", "100")],
        asks=[("0.50", "50")],
    )
    svc.state.target_positions[ADDR1] = {
        TOKEN: Position(token_id=TOKEN, size=Decimal("100"), condition_id="cond-1")
    }

    await svc._on_target_change(_change(after="100"))

    svc.execution.process.assert_called_once_with(
        TOKEN,
        Decimal("5"),
        reason=f"target change {ADDR1} delta=100",
        price_hint=None,
    )


async def test_market_exposure_cap_limits_desired_size() -> None:
    """own_capital × max_market_exposure_pct / 100 / price is binding.

    raw = 500, liquidity_cap = 10000 (deep book)
    risk_cap = 1000 × 5/100 / 0.50 = 100
    → desired = 100, limiting_factor = 'market_risk'
    """
    settings = _settings(
        max_depth_fraction=Decimal("1"),
        max_market_exposure_pct=Decimal("5"),
        default_target_capital_usd=Decimal("1000"),
    )
    svc = _make_service(settings)

    # No bids: price resolves to best_ask = 0.50 (clean risk cap = 100)
    book = OrderBook(TOKEN)
    book.replace(
        bids=[],
        asks=[PriceLevel(Decimal("0.50"), Decimal("10000"))],
    )
    svc.state.books[TOKEN] = book
    svc.state.target_positions[ADDR1] = {
        TOKEN: Position(token_id=TOKEN, size=Decimal("500"), condition_id="cond-1")
    }

    await svc._on_target_change(_change(after="500"))

    svc.execution.process.assert_called_once_with(
        TOKEN,
        Decimal("100"),
        reason=f"target change {ADDR1} delta=500",
        price_hint=None,
    )


# ── reconcile_target tests ────────────────────────────────────────────────────


def _reconciler(state: RuntimeState, on_target_change: AsyncMock) -> Reconciler:
    return Reconciler(
        clob=AsyncMock(),
        data_api=AsyncMock(),
        repository=AsyncMock(),
        events=AsyncMock(),
        state=state,
        own_wallet=None,
        targets=[ADDR1],
        interval_s=30,
        daily_loss_halt_usd=Decimal("50"),
        on_target_change=on_target_change,
    )


async def test_reconcile_target_fires_on_first_appearance() -> None:
    """When DataApi returns positions that aren't in state, on_target_change is called."""
    on_change = AsyncMock()
    state = RuntimeState()
    rec = _reconciler(state, on_change)
    rec.data_api.positions.return_value = [
        Position(
            token_id=TOKEN,
            size=Decimal("10"),
            condition_id="cond-1",
            current_price=Decimal("0.50"),
        )
    ]

    await rec.reconcile_target(ADDR1)

    assert on_change.call_count == 1
    arg: TargetPositionChange = on_change.call_args[0][0]
    assert arg.token_id == TOKEN
    assert arg.address == ADDR1
    assert arg.before_size == ZERO
    assert arg.after_size == Decimal("10")
    assert arg.size_delta == Decimal("10")
    assert arg.price_hint == Decimal("0.50")


async def test_reconcile_drift_refires_target_change() -> None:
    """A second reconcile_target call with updated DataApi positions fires on_target_change again.

    This locks in that the reconciler tracks the latest authoritative snapshot and
    re-triggers the callback whenever it observes drift, not just on the first appearance.
    """
    on_change = AsyncMock()
    state = RuntimeState()
    rec = _reconciler(state, on_change)

    # First snapshot: target holds 10 shares
    rec.data_api.positions.return_value = [
        Position(
            token_id=TOKEN,
            size=Decimal("10"),
            condition_id="cond-1",
            current_price=Decimal("0.50"),
        )
    ]
    await rec.reconcile_target(ADDR1)
    assert on_change.call_count == 1

    # Drift: target now holds 15 shares
    rec.data_api.positions.return_value = [
        Position(
            token_id=TOKEN,
            size=Decimal("15"),
            condition_id="cond-1",
            current_price=Decimal("0.55"),
        )
    ]
    await rec.reconcile_target(ADDR1)

    assert on_change.call_count == 2
    second_arg: TargetPositionChange = on_change.call_args_list[1][0][0]
    assert second_arg.token_id == TOKEN
    assert second_arg.address == ADDR1
    assert second_arg.before_size == Decimal("10")
    assert second_arg.after_size == Decimal("15")
    assert second_arg.size_delta == Decimal("5")
    assert second_arg.price_hint == Decimal("0.55")


async def test_reconcile_target_no_callback_when_positions_unchanged() -> None:
    """on_target_change is NOT called when DataApi matches current state."""
    on_change = AsyncMock()
    state = RuntimeState()
    rec = _reconciler(state, on_change)

    pos = Position(
        token_id=TOKEN,
        size=Decimal("10"),
        condition_id="cond-1",
        current_price=Decimal("0.50"),
    )
    rec.data_api.positions.return_value = [pos]

    await rec.reconcile_target(ADDR1)  # first call: state was empty → fires
    assert on_change.call_count == 1

    # Return exactly the same position again; state and DataApi agree
    await rec.reconcile_target(ADDR1)
    assert on_change.call_count == 1  # no second call
