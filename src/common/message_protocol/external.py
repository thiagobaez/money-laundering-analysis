import json

from asyncio import IncompleteReadError
from enum import IntEnum

from . import external_serializer


class MsgType(IntEnum):
    DATA_BATCH = 1
    RESULT_BATCH_QUERY1 = 2
    RESULT_BATCH_QUERY3 = 3
    RESULT_BATCH_QUERY4 = 4
    RESULT_BATCH_QUERY5 = 5
    ACK = 6
    EOF = 7


RESULT_MSG_TYPES = frozenset(
    {
        MsgType.RESULT_BATCH_QUERY1,
        MsgType.RESULT_BATCH_QUERY3,
        MsgType.RESULT_BATCH_QUERY4,
        MsgType.RESULT_BATCH_QUERY5,
    }
)

_PAYLOAD_MSG_TYPES = RESULT_MSG_TYPES | {
    MsgType.DATA_BATCH,
}


def _recv_sized(socket, size: int) -> bytes:
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def send_msg(socket, msg_type):
    socket.sendall(external_serializer.serialize_uint32(msg_type))


def send_eof(socket):
    send_msg(socket, MsgType.EOF)


def send_batch(socket, batch, msg_type):
    payload = json.dumps(batch).encode("utf-8")

    msg = external_serializer.serialize_uint32(msg_type)
    msg += external_serializer.serialize_uint32(len(payload))
    msg += payload
    socket.sendall(msg)


def recv_batch(payload):
    return json.loads(payload.decode("utf-8"))


def recv_msg(socket) -> tuple:
    msg_type = MsgType(
        external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
    )
    if msg_type in _PAYLOAD_MSG_TYPES:
        size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )

        payload = _recv_sized(socket, size)

        return (msg_type, payload)

    if msg_type == MsgType.EOF:
        return (MsgType.EOF, None)
    if msg_type == MsgType.ACK:
        return (MsgType.ACK, None)
    raise ValueError(f"Unknown message type: {msg_type}")
