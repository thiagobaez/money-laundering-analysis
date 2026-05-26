import functools
from datetime import datetime

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"

_DATASET_NAME_TO_ISO = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "Australian Dollar": "AUD",
    "Yuan": "CNY",
    "Rupee": "INR",
    "Mexican Peso": "MXN",
    "Yen": "JPY",
    "UK Pound": "GBP",
    "Ruble": "RUB",
    "Canadian Dollar": "CAD",
    "Swiss Franc": "CHF",
    "Brazil Real": "BRL",
    "Saudi Riyal": "SAR",
    "Shekel": "ILS",
}


@functools.total_ordering
class TransactionItem:
    def __init__(
        self,
        timestamp: str,
        from_bank: str,
        from_account: str,
        to_bank: str,
        to_account: str,
        amount_received: str,
        receiving_currency: str,
        amount_paid: str,
        payment_currency: str,
        payment_format: str,
        is_laundering: str,
    ):
        self._timestamp = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        self._from_bank = from_bank
        self._from_account = from_account
        self._to_bank = to_bank
        self._to_account = to_account
        self._amount_received = float(amount_received)
        self._receiving_currency = receiving_currency
        self._amount_paid = float(amount_paid)
        self._payment_currency = payment_currency
        self._payment_format = payment_format
        self._is_laundering = int(is_laundering)

    def __eq__(self, other):
        return self._amount_received == other._amount_received

    def __lt__(self, other):
        return self._amount_received < other._amount_received

    def __str__(self):
        return (
            f"timestamp: {self._timestamp}, "
            f"from_bank: {self._from_bank}, "
            f"from_account: {self._from_account}, "
            f"to_bank: {self._to_bank}, "
            f"to_account: {self._to_account}, "
            f"amount_received: {self._amount_received}, "
            f"receiving_currency: {self._receiving_currency}, "
            f"amount_paid: {self._amount_paid}, "
            f"payment_currency: {self._payment_currency}, "
            f"payment_format: {self._payment_format}, "
            f"is_laundering: {self._is_laundering}"
        )

    def is_sent_amount_below(self, max_amount: float) -> bool:
        return self._amount_paid < max_amount

    def is_in_date_range(self, ge_date: str | None, le_date: str | None) -> bool:
        date_str = self._timestamp.date().isoformat()
        return (ge_date is None or date_str >= ge_date) and (
            le_date is None or date_str <= le_date
        )

    def get_payment_format(self) -> str:
        return self._payment_format

    def has_payment_format(self, fmt: str) -> bool:
        return self._payment_format == fmt

    def has_any_payment_format(self, fmts: set) -> bool:
        return self._payment_format in fmts

    def get_from_account(self) -> str:
        return self._from_account

    def get_to_account(self) -> str:
        return self._to_account

    def _currency_to_iso(self, currency_name: str) -> str:
        code = _DATASET_NAME_TO_ISO.get(currency_name)
        if code is None:
            raise ValueError(f"Unknown currency: {currency_name}")
        return code

    def get_payment_currency_iso(self) -> str:
        return self._currency_to_iso(self._payment_currency)

    def get_date_iso(self) -> str:
        return self._timestamp.date().isoformat()

    def get_amount_paid_in_usd(self, rate: float) -> float:
        return self._amount_paid / rate

    def get_amount_paid(self) -> float:
        return self._amount_paid

    def convert_to_usd(self, rate: float) -> None:
        amount_usd = self.get_amount_paid_in_usd(rate)
        self._amount_paid = amount_usd
        self._receiving_currency = "US Dollar"
        self._amount_paid = amount_usd
        self._payment_currency = "US Dollar"

    def is_usd(self) -> bool:
        return self._payment_currency == "US Dollar"

    def is_bitcoin(self) -> bool:
        return self._payment_currency == "Bitcoin"

    def to_fields(self) -> list[str]:
        return [
            self._timestamp.strftime(TIMESTAMP_FORMAT),
            self._from_bank,
            self._from_account,
            self._to_bank,
            self._to_account,
            str(self._amount_received),
            self._receiving_currency,
            str(self._amount_paid),
            self._payment_currency,
            self._payment_format,
            str(self._is_laundering),
        ]
