from asyncio import IncompleteReadError

from . import external_serializer


class MsgType:
    DATA = 1
    ACK = 2
    EOF = 3
    RESULT = 4


def _recv_sized(socket, size: int) -> bytes:
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def send_data(socket, payload: bytes):
    msg = external_serializer.serialize_uint32(MsgType.DATA)
    msg += external_serializer.serialize_uint32(len(payload))
    msg += payload
    socket.sendall(msg)


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
    if msg_type == MsgType.DATA:
        size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        return (MsgType.DATA, _recv_sized(socket, size))
    if msg_type == MsgType.EOF:
        return (MsgType.EOF, None)
    if msg_type == MsgType.ACK:
        return (MsgType.ACK, None)
    if msg_type == MsgType.RESULT:
        size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        return (MsgType.RESULT, _recv_sized(socket, size))
    raise ValueError(f"Unknown message type: {msg_type}")
