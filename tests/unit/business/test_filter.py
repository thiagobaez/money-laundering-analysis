import os
import pytest
from unittest.mock import MagicMock

os.environ.setdefault("ID", "1")
os.environ.setdefault("MOM_HOST", "rabbitmq")
os.environ.setdefault("INPUT_QUEUE", "filter_input")
os.environ.setdefault("OUTPUT_QUEUE", "filter_output")
os.environ.setdefault("MAX_AMOUNT", "50")

from common.message_protocol import internal


def _make_tx_fields(amount):
    return [
        "2023/01/01 12:00",
        "BankA",
        "Acc1",
        "BankB",
        "Acc2",
        str(amount),
        "USD",
        "Wire",
    ]


class TestFilterOnMessage:
    @pytest.fixture
    def filter_worker(self, mocker):
        mock_cls = mocker.patch(
            "business.common_workers.filter.main.middleware.MessageMiddlewareQueueRabbitMQ"
        )
        mock_input, mock_output = MagicMock(), MagicMock()
        mock_cls.side_effect = [mock_input, mock_output]
        from business.common_workers.filter.main import Filter

        return Filter()

    def test_transaction_below_max_is_forwarded(self, filter_worker):
        fields = _make_tx_fields(30.0)
        message = internal.serialize(["client-1", fields])
        ack, nack = MagicMock(), MagicMock()

        filter_worker._on_message(message, ack, nack)

        expected = internal.serialize(["client-1", 1] + fields)
        filter_worker.output_queue.send.assert_called_once_with(expected)
        ack.assert_called_once()
        nack.assert_not_called()

    def test_transaction_above_max_is_discarded(self, filter_worker):
        message = internal.serialize(["client-1", _make_tx_fields(100.0)])
        ack, nack = MagicMock(), MagicMock()

        filter_worker._on_message(message, ack, nack)

        filter_worker.output_queue.send.assert_not_called()
        ack.assert_called_once()
        nack.assert_not_called()

    def test_eof_is_forwarded_to_output(self, filter_worker):
        message = internal.serialize(["client-1"])
        ack, nack = MagicMock(), MagicMock()

        filter_worker._on_message(message, ack, nack)

        filter_worker.output_queue.send.assert_called_once_with(message)
        ack.assert_called_once()
        nack.assert_not_called()

    def test_malformed_transaction_nacks(self, filter_worker):
        # Too few fields → TypeError in TransactionItem constructor
        message = internal.serialize(["client-1", ["only-one-field"]])
        ack, nack = MagicMock(), MagicMock()

        filter_worker._on_message(message, ack, nack)

        nack.assert_called_once()
        ack.assert_not_called()
