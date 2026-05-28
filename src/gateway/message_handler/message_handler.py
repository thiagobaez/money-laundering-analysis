from common.message_protocol import internal
import uuid


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_tx(self, fields):
        return internal.serialize([self.client_id] + list(fields))

    def serialize_tx_batch(self, rows):
        return internal.serialize([self.client_id] + rows)

    def serialize_eof(self):
        return internal.serialize([self.client_id])

    def deserialize_result(self, message):
        deserialized = internal.deserialize(message)
        if not isinstance(deserialized, list):
            return None
        if len(deserialized) < 2 or deserialized[0] != self.client_id:
            return None
        return tuple(deserialized[1:])
