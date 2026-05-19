import pytest
from datetime import datetime
from common.transaction_item import TransactionItem

BASE = {
    "timestamp": "2022/09/01 00:16",
    "from_bank": "HSBC",
    "from_account": "123",
    "to_bank": "Santander",
    "to_account": "456",
    "amount_received": "100.0",
    "receiving_currency": "US Dollar",
    "amount_paid": "100.0",
    "payment_currency": "US Dollar",
    "payment_format": "Reinvestment",
    "is_laundering": "0",
}


def make(**overrides):
    return TransactionItem(**{**BASE, **overrides})


class TestInit:
    def test_timestamp_parsed(self):
        item = make()
        assert item.is_between(datetime(2022, 9, 1, 0, 16), datetime(2022, 9, 1, 0, 16))

    def test_amount_is_float(self):
        item = make()
        assert item.is_sent_amount_below(100.01)
        assert not item.is_sent_amount_below(99.99)

    def test_invalid_timestamp_raises(self):
        with pytest.raises(ValueError):
            make(timestamp="bad")


class TestComparison:
    def test_eq_same_amount(self):
        assert make() == make()

    def test_lt_lower_amount(self):
        assert make(amount_paid="50.0") < make(amount_paid="200.0")

    def test_gt_higher_amount(self):
        assert make(amount_paid="200.0") > make(amount_paid="50.0")

    def test_le_same_amount(self):
        assert make() <= make()

    def test_ge_same_amount(self):
        assert make() >= make()


class TestIsBetween:
    def test_within_range(self):
        item = make()
        assert item.is_between(datetime(2022, 1, 1), datetime(2022, 12, 31))

    def test_on_boundary(self):
        item = make()
        ts = datetime(2022, 9, 1, 0, 16)
        assert item.is_between(ts, ts)

    def test_outside_range(self):
        item = make()
        assert not item.is_between(datetime(2023, 1, 1), datetime(2023, 12, 31))


class TestHasPaymentFormat:
    def test_matching_format(self):
        item = make()
        assert item.has_payment_format("Reinvestment")

    def test_non_matching_format(self):
        item = make()
        assert not item.has_payment_format("Cash")


class TestIsUsd:
    def test_usd_currency(self):
        item = make()
        assert item.is_usd()

    def test_non_usd_currency(self):
        item = make(payment_currency="EUR")
        assert not item.is_usd()
