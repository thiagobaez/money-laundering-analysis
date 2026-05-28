import pytest
from unittest.mock import MagicMock
from asyncio import IncompleteReadError

from common.message_protocol import external_serializer
from common.message_protocol.external import (
    MsgType,
    _recv_sized,
    send_data,
    recv_data,
    send_eof,
    recv_msg,
)


def _make_socket(*data_chunks):
    data = b"".join(data_chunks)
    offset = [0]

    def recv_into(buf):
        n = min(len(buf), len(data) - offset[0])
        buf[:n] = data[offset[0] : offset[0] + n]
        offset[0] += n
        return n

    sock = MagicMock()
    sock.recv_into.side_effect = recv_into
    return sock


# --- external_serializer ---


class TestUint32Roundtrip:
    def test_zero(self):
        assert (
            external_serializer.deserialize_uint32(
                external_serializer.serialize_uint32(0)
            )
            == 0
        )

    def test_max(self):
        assert (
            external_serializer.deserialize_uint32(
                external_serializer.serialize_uint32(2**32 - 1)
            )
            == 2**32 - 1
        )

    def test_arbitrary(self):
        assert (
            external_serializer.deserialize_uint32(
                external_serializer.serialize_uint32(12345)
            )
            == 12345
        )


class TestStringRoundtrip:
    def test_empty(self):
        assert (
            external_serializer.deserialize_string(
                external_serializer.serialize_string("")
            )
            == ""
        )

    def test_ascii(self):
        assert (
            external_serializer.deserialize_string(
                external_serializer.serialize_string("hello")
            )
            == "hello"
        )

    def test_unicode(self):
        assert (
            external_serializer.deserialize_string(
                external_serializer.serialize_string("árbol")
            )
            == "árbol"
        )


# --- external.py ---


class TestSendData:
    def test_sends_correct_bytes(self):
        sock = MagicMock()
        payload = b"hello"
        send_data(sock, payload)
        expected = (
            external_serializer.serialize_uint32(MsgType.DATA)
            + external_serializer.serialize_uint32(len(payload))
            + payload
        )
        sock.sendall.assert_called_once_with(expected)


class TestRecvData:
    def test_reads_type_and_returns_payload(self):
        payload = b"world"
        data = (
            external_serializer.serialize_uint32(MsgType.DATA)
            + external_serializer.serialize_uint32(len(payload))
            + payload
        )
        assert recv_data(_make_socket(data)) == payload

    def test_raises_if_type_is_not_data(self):
        data = external_serializer.serialize_uint32(MsgType.EOF)
        with pytest.raises(ValueError, match="Expected DATA"):
            recv_data(_make_socket(data))


class TestSendEof:
    def test_sends_correct_bytes(self):
        sock = MagicMock()
        send_eof(sock)
        expected = external_serializer.serialize_uint32(MsgType.EOF)
        sock.sendall.assert_called_once_with(expected)


class TestRecvMsg:
    def _frame(self, msg_type, payload=None):
        buf = external_serializer.serialize_uint32(msg_type)
        if payload is not None:
            buf += external_serializer.serialize_uint32(len(payload))
            buf += payload
        return buf

    def test_data_message(self):
        payload = b"data-payload"
        assert recv_msg(_make_socket(self._frame(MsgType.DATA, payload))) == (
            MsgType.DATA,
            payload,
        )

    def test_eof_message(self):
        assert recv_msg(_make_socket(self._frame(MsgType.EOF))) == (MsgType.EOF, None)

    def test_ack_message(self):
        assert recv_msg(_make_socket(self._frame(MsgType.ACK))) == (MsgType.ACK, None)

    @pytest.mark.parametrize(
        "msg_type",
        [
            MsgType.RESULT_QUERY1,
            MsgType.RESULT_QUERY3,
            MsgType.RESULT_QUERY4,
            MsgType.RESULT_QUERY5,
        ],
    )
    def test_result_query_messages(self, msg_type):
        payload = b"result-payload"
        assert recv_msg(_make_socket(self._frame(msg_type, payload))) == (
            msg_type,
            payload,
        )

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError):
            recv_msg(_make_socket(external_serializer.serialize_uint32(99)))


class TestRecvSized:
    def test_raises_incomplete_read_error_on_socket_close(self):
        sock = MagicMock()
        sock.recv_into.return_value = 0
        with pytest.raises(IncompleteReadError):
            _recv_sized(sock, 4)

    def test_assembles_partial_reads(self):
        # recv_into delivers data in two chunks to exercise the loop
        chunks = [b"\x00\x01", b"\x02\x03"]
        idx = [0]

        def recv_into(buf):
            chunk = chunks[idx[0]]
            idx[0] += 1
            n = min(len(buf), len(chunk))
            buf[:n] = chunk[:n]
            return n

        sock = MagicMock()
        sock.recv_into.side_effect = recv_into
        assert _recv_sized(sock, 4) == b"\x00\x01\x02\x03"


# --- internal.py ---


class TestInternal:
    def test_dict_roundtrip(self):
        from common.message_protocol.internal import deserialize, serialize

        data = {"key": "value", "num": 42}
        assert deserialize(serialize(data)) == data

    def test_list_roundtrip(self):
        from common.message_protocol.internal import deserialize, serialize

        data = [1, 2, 3, "four"]
        assert deserialize(serialize(data)) == data

    def test_string_roundtrip(self):
        from common.message_protocol.internal import deserialize, serialize

        assert deserialize(serialize("hello world")) == "hello world"

    def test_int_roundtrip(self):
        from common.message_protocol.internal import deserialize, serialize

        assert deserialize(serialize(12345)) == 12345
