from decimal import Decimal

from syncopy.domain import Side, SizingInput
from syncopy.strategy import PositionSizer, marketable_limit


def test_sizer_uses_tightest_cap() -> None:
    sizer = PositionSizer(max_depth_fraction=Decimal("0.1"), max_market_exposure_pct=Decimal("5"))
    result = sizer.size(
        SizingInput(
            target_size=Decimal("1000"),
            target_capital=Decimal("10000"),
            own_capital=Decimal("1000"),
            market_price=Decimal("0.5"),
            depth_shares=Decimal("200"),
        )
    )
    assert result.raw_mirror == Decimal("100")
    assert result.liquidity_cap == Decimal("20.0")
    assert result.risk_cap_shares == Decimal("1.0E+2")
    assert result.desired_size == Decimal("20.0")
    assert result.limiting_factor == "liquidity"


def test_multi_target_aggregation() -> None:
    sizer = PositionSizer(max_depth_fraction=Decimal("1"), max_market_exposure_pct=Decimal("100"))
    result = sizer.size_aggregate(
        [
            SizingInput(
                Decimal("100"),
                Decimal("1000"),
                Decimal("1000"),
                Decimal("0.5"),
                Decimal("1000"),
                Decimal("0.5"),
            ),
            SizingInput(
                Decimal("200"),
                Decimal("2000"),
                Decimal("1000"),
                Decimal("0.5"),
                Decimal("1000"),
            ),
        ]
    )
    assert result.raw_mirror == Decimal("150.0")
    assert result.desired_size == Decimal("150.0")


def test_marketable_limit_is_tick_aligned_and_bounded() -> None:
    assert marketable_limit(
        side=Side.BUY,
        best_price=Decimal("0.51"),
        tick_size=Decimal("0.01"),
        buffer_ticks=1,
    ) == Decimal("0.52")
    assert marketable_limit(
        side=Side.SELL,
        best_price=Decimal("0.01"),
        tick_size=Decimal("0.01"),
        buffer_ticks=2,
    ) == Decimal("0.01")
