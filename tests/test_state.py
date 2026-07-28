from decimal import Decimal

from syncopy.domain import Freshness, PriceLevel, Side
from syncopy.state import OrderBook


def test_order_book_depth_and_fill() -> None:
    book = OrderBook("token")
    book.replace(
        [PriceLevel(Decimal("0.49"), Decimal("20"))],
        [
            PriceLevel(Decimal("0.50"), Decimal("10")),
            PriceLevel(Decimal("0.51"), Decimal("15")),
            PriceLevel(Decimal("0.54"), Decimal("100")),
        ],
    )
    assert book.freshness is Freshness.FRESH
    assert book.depth_within(Side.BUY, Decimal("2")) == Decimal("25")
    filled, average = book.simulate_fill(Side.BUY, Decimal("20"), Decimal("0.51"))
    assert filled == Decimal("20")
    assert average == Decimal("0.505")


def test_zero_size_deletes_level() -> None:
    book = OrderBook("token")
    book.replace([PriceLevel(Decimal("0.4"), Decimal("2"))], [])
    book.update_level(Side.BUY, Decimal("0.4"), Decimal("0"))
    assert book.best_bid is None


def test_consumed_fill_decrements_opposing_depth() -> None:
    book = OrderBook("token")
    book.replace([], [PriceLevel(Decimal("0.5"), Decimal("10"))])
    filled, _ = book.consume_fill(Side.BUY, Decimal("4"), Decimal("0.5"))
    assert filled == Decimal("4")
    assert book.asks[Decimal("0.5")] == Decimal("6")
