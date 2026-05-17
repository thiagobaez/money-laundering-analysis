from common.message_protocol import internal
import uuid


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_tx(self, fields):
        return internal.serialize([self.client_id] + list(fields))

    def serialize_eof(self):
        # EOF is [client_id] with no data fields so the receiver can route it
        return internal.serialize([self.client_id])

    def deserialize_result(self, message):
        deserialized = internal.deserialize(message)
        # A non-list frame is malformed; ignore it
        if not isinstance(deserialized, list):
            return None
        # A list with only [client_id] and no data fields is the EOF signal
        # A list with a different client_id belongs to another gateway instance
        if len(deserialized) < 2 or deserialized[0] != self.client_id:
            return None
        return tuple(deserialized[1:])
