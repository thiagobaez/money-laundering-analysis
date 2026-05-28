import json
import zlib


def serialize(message):
    return zlib.compress(json.dumps(message).encode("utf-8"))


def deserialize(message):
    return json.loads(zlib.decompress(message).decode("utf-8"))
