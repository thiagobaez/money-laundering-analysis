import functools
from datetime import datetime

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"


@functools.total_ordering
class TransactionItem:
    def __init__(
        self,
        timestamp: str,
        from_bank: str,
        from_account: str,
        to_bank: str,
        to_account: str,
        amount_paid: str,
        payment_currency: str,
        payment_format: str,
    ):
        self.timestamp = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        self.from_bank = from_bank
        self.from_account = from_account
        self.to_bank = to_bank
        self.to_account = to_account
        self.amount = float(amount_paid)
        self.currency = payment_currency
        self.payment_format = payment_format

    def __eq__(self, other):
        return self.amount == other.amount

    def __lt__(self, other):
        return self.amount < other.amount

    def is_between(self, date_from: datetime, date_to: datetime) -> bool:
        return date_from <= self.timestamp <= date_to

    def has_payment_format(self, fmt: str) -> bool:
        return self.payment_format == fmt

    def is_usd(self) -> bool:
        return self.currency == "USD"
