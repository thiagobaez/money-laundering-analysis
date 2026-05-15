from common import message_protocol
import uuid


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_tx_message(self, message):
        [
            timestamp,
            from_bank,
            from_account,
            to_bank,
            to_account,
            amount_paid,
            payment_currency,
            payment_format,
        ] = message
        TransactionMessage = message_protocol.internal.TransactionMessage(
            timestamp=timestamp,
            from_bank=from_bank,
            from_account=from_account,
            to_bank=to_bank,
            to_account=to_account,
            amount_paid=amount_paid,
            payment_currency=payment_currency,
            payment_format=payment_format,
        )
        return message_protocol.internal.serialize([self.client_id, TransactionMessage])

    def serialize_acc_message(self, message):
        [bank_name, bank_id] = message
        BankMessage = message_protocol.internal.BankMessage(
            bank_name=bank_name, bank_id=bank_id
        )
        return message_protocol.internal.serialize([self.client_id, BankMessage])

    def serialize_eof_message(self):
        return message_protocol.internal.serialize([self.client_id])
