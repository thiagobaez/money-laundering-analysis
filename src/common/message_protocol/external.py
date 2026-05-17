from asyncio import IncompleteReadError

from . import external_serializer


class MsgType:
    DATA = 1
    ACK = 2
    EOF = 3
    RESULT_QUERY1 = 4
    RESULT_QUERY3 = 5
    RESULT_QUERY4 = 6
    RESULT_QUERY5 = 7


RESULT_MSG_TYPES = frozenset(
    {
        MsgType.RESULT_QUERY1,
        MsgType.RESULT_QUERY3,
        MsgType.RESULT_QUERY4,
        MsgType.RESULT_QUERY5,
    }
)

_PAYLOAD_MSG_TYPES = RESULT_MSG_TYPES | {MsgType.DATA}


def _recv_sized(socket, size: int) -> bytes:
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def send_data(socket, payload: bytes, msg_type=MsgType.DATA):
    msg = external_serializer.serialize_uint32(msg_type)
    msg += external_serializer.serialize_uint32(len(payload))
    msg += payload
    socket.sendall(msg)


def send_msg(socket, msg_type):
    """Send a header-only message with no payload (ACK, EOF)."""
    socket.sendall(external_serializer.serialize_uint32(msg_type))


def recv_data(socket) -> bytes:
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    if msg_type != MsgType.DATA:
        raise ValueError(f"Expected DATA, got {msg_type}")
    size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return _recv_sized(socket, size)


def send_eof(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.EOF))


def recv_msg(socket) -> tuple:
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    if msg_type in _PAYLOAD_MSG_TYPES:
        size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        return (msg_type, _recv_sized(socket, size))
    if msg_type == MsgType.EOF:
        return (MsgType.EOF, None)
    if msg_type == MsgType.ACK:
        return (MsgType.ACK, None)
    raise ValueError(f"Unknown message type: {msg_type}")
