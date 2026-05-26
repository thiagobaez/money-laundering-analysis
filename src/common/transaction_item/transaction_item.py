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
        self._timestamp = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        self._from_bank = from_bank
        self._from_account = from_account
        self._to_bank = to_bank
        self._to_account = to_account
        self._amount_paid = float(amount_paid)
        self._payment_currency = payment_currency
        self._payment_format = payment_format

    def __eq__(self, other):
        return self._amount_paid == other._amount_paid

    def __lt__(self, other):
        return self._amount_paid < other._amount_paid

    def __str__(self):
        return (
            f"timestamp: {self._timestamp}, "
            f"from_bank: {self._from_bank}, "
            f"from_account: {self._from_account}, "
            f"to_bank: {self._to_bank}, "
            f"to_account: {self._to_account}, "
            f"amount_paid: {self._amount_paid}, "
            f"payment_currency: {self._payment_currency}, "
            f"payment_format: {self._payment_format}"
        )

    def is_sent_amount_below(self, max_amount: float) -> bool:
        return self._amount_paid < max_amount

    def is_in_date_range(self, ge_date: str | None, le_date: str | None) -> bool:
        date_str = self._timestamp.date().isoformat()
        return (ge_date is None or date_str >= ge_date) and (le_date is None or date_str <= le_date)

    def has_payment_format(self, fmt: str) -> bool:
        return self._payment_format == fmt

    def has_any_payment_format(self, fmts: set) -> bool:
        return self._payment_format in fmts
    
    def get_from_account(self) -> str:
        return self._from_account
    
    def get_to_account(self) -> str:
        return self._to_account

    def is_usd(self) -> bool:
        return self._payment_currency == "US Dollar"

    def is_between(self, date_from: datetime, date_to: datetime) -> bool:
        return date_from <= self._timestamp <= date_to

    def to_fields(self) -> list[str]:
        return [
            self._timestamp.strftime(TIMESTAMP_FORMAT),
            self._from_bank,
            self._from_account,
            self._to_bank,
            self._to_account,
            str(self._amount_paid),
            self._payment_currency,
            self._payment_format,
        ]

