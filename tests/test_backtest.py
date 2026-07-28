from decimal import Decimal

from syncopy.backtest import ReplayEngine

ADDRESS = "0x1111111111111111111111111111111111111111"


def test_replay_uses_book_depth_and_strategy() -> None:
    engine = ReplayEngine(
        own_capital=Decimal("1000"),
        target_capitals={ADDRESS: Decimal("1000")},
        target_weights={ADDRESS: Decimal("1")},
        default_target_capital=Decimal("1000"),
        max_depth_fraction=Decimal("1"),
        max_market_exposure_pct=Decimal("100"),
        slippage_tolerance_pct=Decimal("2"),
    )
    report = engine.replay(
        [
            {
                "type": "book",
                "payload": {
                    "asset_id": "token",
                    "bids": [{"price": "0.49", "size": "100"}],
                    "asks": [{"price": "0.50", "size": "100"}],
                },
            },
            {
                "ts": "2026-01-01T00:00:00Z",
                "type": "target_position_change",
                "payload": {
                    "address": ADDRESS,
                    "token_id": "token",
                    "after_size": "10",
                },
            },
        ]
    )
    assert len(report.trades) == 1
    assert report.trades[0].size == "10"
    # Marked at midpoint after buying at the ask, so the spread is immediately visible.
    assert report.ending_equity == Decimal("999.950")
