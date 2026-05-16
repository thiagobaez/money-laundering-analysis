from common import message_protocol
import uuid

class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        return message_protocol.internal.serialize([self.client_id, message])

    def serialize_eof_message(self):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        deserialized = message_protocol.internal.deserialize(message)
        if deserialized[0] != self.client_id:
            return None, False
        if len(deserialized) == 1:
            return None, True  # EOF from filter
        return deserialized[1].encode("utf-8"), False