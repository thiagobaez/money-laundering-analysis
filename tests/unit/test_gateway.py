import os
import socket

os.environ.setdefault("SERVER_HOST", "localhost")
os.environ.setdefault("SERVER_PORT", "5678")
os.environ.setdefault("MOM_HOST", "rabbitmq")
os.environ.setdefault("INPUT_QUEUE", "input")
os.environ.setdefault("OUTPUT_QUEUE", "output")

from unittest.mock import MagicMock
from gateway.main import (
    MessageHandler,
    handle_client_request,
    handle_client_response,
    handle_sigterm,
)
from common.message_protocol import internal, external
from common.message_protocol.external import MsgType
from common.message_protocol import external_serializer


def _make_fake_socket(*messages):
    """Build a mock socket whose recv_into feeds framed messages sequentially.

    Each element of messages is either (MsgType.DATA, payload_bytes) or
    (MsgType.EOF, None).
    """
    buf = bytearray()
    for msg_type, payload in messages:
        buf += external_serializer.serialize_uint32(msg_type)
        if msg_type == MsgType.DATA:
            buf += external_serializer.serialize_uint32(len(payload))
            buf += payload

    data = bytes(buf)
    pos = [0]

    def recv_into(mv):
        n = len(mv)
        chunk = data[pos[0] : pos[0] + n]
        mv[: len(chunk)] = chunk
        pos[0] += len(chunk)
        return len(chunk)

    sock = MagicMock()
    sock.recv_into.side_effect = recv_into
    return sock


class TestMessageHandler:
    def test_serialize_tx_includes_client_id(self):
        handler = MessageHandler()
        result = internal.deserialize(handler.serialize_tx(["field1", "field2"]))
        assert result[0] == handler.client_id
        assert result[1:] == ["field1", "field2"]

    def test_serialize_eof_is_eof_message(self):
        handler = MessageHandler()
        result = internal.deserialize(handler.serialize_eof())
        assert result == internal.EOF_MESSAGE

    def test_deserialize_result_returns_tuple_for_matching_client(self):
        handler = MessageHandler()
        message = internal.serialize([handler.client_id, 2, "some_data"])
        assert handler.deserialize_result(message) == (2, "some_data")

    def test_deserialize_result_returns_none_for_different_client(self):
        handler = MessageHandler()
        message = internal.serialize(["other-client-id", 2, "some_data"])
        assert handler.deserialize_result(message) is None

    def test_deserialize_result_returns_none_for_eof_message(self):
        handler = MessageHandler()
        message = internal.serialize(internal.EOF_MESSAGE)
        assert handler.deserialize_result(message) is None

    def test_deserialize_result_returns_none_for_short_list(self):
        handler = MessageHandler()
        message = internal.serialize([handler.client_id])
        assert handler.deserialize_result(message) is None


class TestHandleClientRequest:
    def test_data_message_is_published_and_ack_sent(self, mocker):
        mock_queue_cls = mocker.patch("gateway.main.MessageMiddlewareQueueRabbitMQ")
        mock_queue = mock_queue_cls.return_value

        handler = MessageHandler()
        payload = internal.serialize(["field1", "field2"])
        client_socket = _make_fake_socket(
            (MsgType.DATA, payload),
            (MsgType.EOF, None),
        )

        handle_client_request(client_socket, handler)

        expected_tx = handler.serialize_tx(internal.deserialize(payload))
        expected_eof = handler.serialize_eof()

        assert mock_queue.send.call_count == 2
        mock_queue.send.assert_any_call(expected_tx)
        mock_queue.send.assert_any_call(expected_eof)

        client_socket.sendall.assert_called()
        mock_queue.close.assert_called_once()

    def test_socket_error_logs_and_closes_queue(self, mocker):
        mock_queue_cls = mocker.patch("gateway.main.MessageMiddlewareQueueRabbitMQ")
        mock_queue = mock_queue_cls.return_value

        handler = MessageHandler()
        client_socket = MagicMock()
        client_socket.recv_into.side_effect = socket.error("connection reset")

        handle_client_request(client_socket, handler)

        mock_queue.close.assert_called_once()


class TestHandleClientResponse:
    def test_result_sent_to_matching_client(self, mocker):
        mock_queue_cls = mocker.patch("gateway.main.MessageMiddlewareQueueRabbitMQ")
        mock_queue = mock_queue_cls.return_value
        mock_send_data = mocker.patch("gateway.main.external.send_data")

        handler = MessageHandler()
        sock = MagicMock()
        client_map = {handler.client_id: [handler, sock]}

        message = internal.serialize([handler.client_id, 2, "some_data"])
        ack = MagicMock()
        nack = MagicMock()

        captured = {}

        def fake_start_consuming(cb):
            captured["cb"] = cb

        mock_queue.start_consuming.side_effect = fake_start_consuming

        handle_client_response(client_map)

        captured["cb"](message, ack, nack)

        mock_send_data.assert_called_once_with(
            sock, internal.serialize([2, "some_data"])
        )
        ack.assert_called_once()
        nack.assert_not_called()

    def test_nack_called_when_no_matching_client(self, mocker):
        mock_queue_cls = mocker.patch("gateway.main.MessageMiddlewareQueueRabbitMQ")
        mock_queue = mock_queue_cls.return_value

        handler = MessageHandler()
        sock = MagicMock()
        client_map = {"other-client-id": [handler, sock]}

        message = internal.serialize(["unrelated-id", 2, "some_data"])
        ack = MagicMock()
        nack = MagicMock()

        captured = {}

        def fake_start_consuming(cb):
            captured["cb"] = cb

        mock_queue.start_consuming.side_effect = fake_start_consuming

        handle_client_response(client_map)

        captured["cb"](message, ack, nack)

        ack.assert_not_called()
        nack.assert_called_once()

    def test_socket_error_removes_client_and_acks(self, mocker):
        mock_queue_cls = mocker.patch("gateway.main.MessageMiddlewareQueueRabbitMQ")
        mock_queue = mock_queue_cls.return_value
        mocker.patch(
            "gateway.main.external.send_data", side_effect=socket.error("broken pipe")
        )

        handler = MessageHandler()
        sock = MagicMock()
        client_map = {handler.client_id: [handler, sock]}

        message = internal.serialize([handler.client_id, 2, "some_data"])
        ack = MagicMock()
        nack = MagicMock()

        captured = {}

        def fake_start_consuming(cb):
            captured["cb"] = cb

        mock_queue.start_consuming.side_effect = fake_start_consuming

        handle_client_response(client_map)

        captured["cb"](message, ack, nack)

        assert handler.client_id not in client_map
        ack.assert_called_once()
        nack.assert_not_called()


class TestHandleSigterm:
    def test_sigterm_shuts_down_server_and_clients(self):
        server_socket = MagicMock()
        sock1 = MagicMock()
        client_map = {"id1": (MagicMock(), sock1)}
        sigterm_received = MagicMock()

        handle_sigterm(server_socket, client_map, sigterm_received)

        server_socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        sock1.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        assert sigterm_received.value == 1
