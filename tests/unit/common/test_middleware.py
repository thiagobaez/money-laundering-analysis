import pytest
from unittest.mock import MagicMock, call

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
)


@pytest.fixture(autouse=True)
def mock_pika(mocker):
    return mocker.patch("common.middleware.middleware_rabbitmq.pika")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_queue(mock_pika, host="localhost", queue_name="test-queue"):
    return MessageMiddlewareQueueRabbitMQ(host=host, queue_name=queue_name)


def _make_exchange(
    mock_pika, host="localhost", exchange_name="test-exchange", routing_keys=None
):
    if routing_keys is None:
        routing_keys = ["k1", "k2"]
    return MessageMiddlewareExchangeRabbitMQ(
        host=host, exchange_name=exchange_name, routing_keys=routing_keys
    )


# ===========================================================================
# TestMessageMiddlewareQueueRabbitMQ
# ===========================================================================


class TestMessageMiddlewareQueueRabbitMQ:
    def test_init_connects_and_declares_queue(self, mock_pika):
        host = "rabbitmq-host"
        queue_name = "my-queue"

        _make_queue(mock_pika, host=host, queue_name=queue_name)

        mock_pika.BlockingConnection.assert_called_once_with(
            mock_pika.ConnectionParameters(host=host)
        )
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        mock_pika.BlockingConnection.return_value.channel.assert_called_once()
        channel.queue_declare.assert_called_once_with(queue=queue_name, durable=True)

    def test_init_raises_runtime_error_on_connection_failure(self, mock_pika):
        mock_pika.exceptions.AMQPConnectionError = Exception
        mock_pika.BlockingConnection.side_effect = Exception("connection refused")

        with pytest.raises(RuntimeError):
            _make_queue(mock_pika)

    def test_start_consuming_registers_and_starts(self, mock_pika):
        queue_name = "my-queue"
        mw = _make_queue(mock_pika, queue_name=queue_name)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value

        user_callback = MagicMock()
        mw.start_consuming(user_callback)

        channel.basic_consume.assert_called_once_with(
            queue=queue_name,
            on_message_callback=channel.basic_consume.call_args.kwargs[
                "on_message_callback"
            ],
            auto_ack=False,
        )
        channel.start_consuming.assert_called_once()

    def test_start_consuming_callback_receives_body_ack_nack(self, mock_pika):
        mw = _make_queue(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value

        user_callback = MagicMock()
        mw.start_consuming(user_callback)

        # Extract the internal on_message callback passed to basic_consume
        on_message = channel.basic_consume.call_args.kwargs["on_message_callback"]

        fake_channel = MagicMock()
        fake_method = MagicMock()
        fake_properties = MagicMock()
        body = b"hello"

        on_message(fake_channel, fake_method, fake_properties, body)

        # User callback receives (body, ack_fn, nack_fn)
        assert user_callback.call_count == 1
        received_body, ack_fn, nack_fn = user_callback.call_args.args

        assert received_body == body

        ack_fn()
        fake_channel.basic_ack.assert_called_once_with(fake_method.delivery_tag)

        nack_fn()
        fake_channel.basic_nack.assert_called_once_with(fake_method.delivery_tag)

    def test_stop_consuming_does_not_call_close(self, mock_pika):
        mw = _make_queue(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        connection = mock_pika.BlockingConnection.return_value

        mw.stop_consuming()

        channel.stop_consuming.assert_called_once()
        channel.close.assert_not_called()
        connection.close.assert_not_called()

    def test_send_publishes_with_correct_args(self, mock_pika):
        queue_name = "my-queue"
        mw = _make_queue(mock_pika, queue_name=queue_name)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value

        mw.send(b"payload")

        channel.basic_publish.assert_called_once_with(
            exchange="",
            routing_key=queue_name,
            body=b"payload",
            properties=mock_pika.BasicProperties.return_value,
        )
        mock_pika.BasicProperties.assert_called_with(delivery_mode=2)

    def test_close_closes_channel_and_connection_when_open(self, mock_pika):
        mw = _make_queue(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        connection = mock_pika.BlockingConnection.return_value

        channel.is_open = True
        connection.is_open = True

        mw.close()

        channel.close.assert_called_once()
        connection.close.assert_called_once()

    def test_close_skips_closed_channel_and_connection(self, mock_pika):
        mw = _make_queue(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        connection = mock_pika.BlockingConnection.return_value

        channel.is_open = False
        connection.is_open = False

        mw.close()

        channel.close.assert_not_called()
        connection.close.assert_not_called()


# ===========================================================================
# TestMessageMiddlewareExchangeRabbitMQ
# ===========================================================================


class TestMessageMiddlewareExchangeRabbitMQ:
    def test_init_declares_exchange_and_binds_keys(self, mock_pika):
        exchange_name = "my-exchange"
        routing_keys = ["k1", "k2"]

        _make_exchange(
            mock_pika, exchange_name=exchange_name, routing_keys=routing_keys
        )

        channel = mock_pika.BlockingConnection.return_value.channel.return_value

        channel.exchange_declare.assert_called_once_with(
            exchange=exchange_name, exchange_type="direct"
        )
        channel.queue_declare.assert_called_once_with(queue="", exclusive=True)

        queue_name = channel.queue_declare.return_value.method.queue
        assert channel.queue_bind.call_count == len(routing_keys)
        channel.queue_bind.assert_has_calls(
            [
                call(exchange=exchange_name, queue=queue_name, routing_key="k1"),
                call(exchange=exchange_name, queue=queue_name, routing_key="k2"),
            ],
            any_order=False,
        )

    def test_init_raises_runtime_error_on_connection_failure(self, mock_pika):
        mock_pika.exceptions.AMQPConnectionError = Exception
        mock_pika.BlockingConnection.side_effect = Exception("connection refused")

        with pytest.raises(RuntimeError):
            _make_exchange(mock_pika)

    def test_stop_consuming_does_not_call_close(self, mock_pika):
        mw = _make_exchange(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        connection = mock_pika.BlockingConnection.return_value

        mw.stop_consuming()

        channel.stop_consuming.assert_called_once()
        channel.close.assert_not_called()
        connection.close.assert_not_called()

    def test_send_without_routing_key_publishes_to_all_keys(self, mock_pika):
        routing_keys = ["k1", "k2"]
        exchange_name = "my-exchange"
        mw = _make_exchange(
            mock_pika, exchange_name=exchange_name, routing_keys=routing_keys
        )
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.reset_mock()

        mw.send(b"payload")

        assert channel.basic_publish.call_count == 2
        channel.basic_publish.assert_has_calls(
            [
                call(
                    exchange=exchange_name,
                    routing_key="k1",
                    body=b"payload",
                    properties=mock_pika.BasicProperties.return_value,
                ),
                call(
                    exchange=exchange_name,
                    routing_key="k2",
                    body=b"payload",
                    properties=mock_pika.BasicProperties.return_value,
                ),
            ],
            any_order=False,
        )
        mock_pika.BasicProperties.assert_called_with(delivery_mode=2)

    def test_send_with_routing_key_publishes_to_one_key(self, mock_pika):
        exchange_name = "my-exchange"
        mw = _make_exchange(
            mock_pika, exchange_name=exchange_name, routing_keys=["k1", "k2"]
        )
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.reset_mock()

        mw.send(b"payload", routing_key="k1")

        channel.basic_publish.assert_called_once_with(
            exchange=exchange_name,
            routing_key="k1",
            body=b"payload",
            properties=mock_pika.BasicProperties.return_value,
        )
        mock_pika.BasicProperties.assert_called_with(delivery_mode=2)

    def test_close_closes_channel_and_connection_when_open(self, mock_pika):
        mw = _make_exchange(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        connection = mock_pika.BlockingConnection.return_value

        channel.is_open = True
        connection.is_open = True

        mw.close()

        channel.close.assert_called_once()
        connection.close.assert_called_once()

    def test_close_skips_closed_channel_and_connection(self, mock_pika):
        mw = _make_exchange(mock_pika)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        connection = mock_pika.BlockingConnection.return_value

        channel.is_open = False
        connection.is_open = False

        mw.close()

        channel.close.assert_not_called()
        connection.close.assert_not_called()
