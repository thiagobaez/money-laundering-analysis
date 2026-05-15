UINT32_SIZE = 4


def serialize_uint32(u: int) -> bytes:
    return u.to_bytes(UINT32_SIZE, "big")


def deserialize_uint32(b: bytes) -> int:
    return int.from_bytes(b, byteorder="big", signed=False)
