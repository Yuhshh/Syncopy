from decimal import Decimal

from syncopy.messages import DataPosition, WsPriceChange


def test_current_position_api_shape() -> None:
    item = DataPosition.model_validate(
        {
            "proxyWallet": "0x1111111111111111111111111111111111111111",
            "asset": "123",
            "conditionId": "0xabc",
            "size": 42,
            "avgPrice": 0.45,
            "curPrice": 0.5,
        }
    )
    assert item.asset == "123"
    assert item.avg_price == Decimal("0.45")


def test_ws_price_change_shape() -> None:
    event = WsPriceChange.model_validate(
        {
            "event_type": "price_change",
            "market": "0xmarket",
            "timestamp": "123",
            "price_changes": [{"asset_id": "1", "price": "0.5", "size": "20", "side": "BUY"}],
        }
    )
    assert event.price_changes[0].size == Decimal("20")
