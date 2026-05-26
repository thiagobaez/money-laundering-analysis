import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]

INPUT_QUEUE = os.environ.get("INPUT_QUEUE")
OUTPUT_QUEUES = (
    os.environ.get("OUTPUT_QUEUES", "").split(",")
    if os.environ.get("OUTPUT_QUEUES")
    else None
)

INPUT_EXCHANGE_NAME = os.environ.get("INPUT_EXCHANGE_NAME")
INPUT_ROUTING_KEYS = (
    os.environ.get("INPUT_ROUTING_KEYS", "").split(",")
    if os.environ.get("INPUT_ROUTING_KEYS")
    else None
)

OUTPUT_EXCHANGE_NAME = os.environ.get("OUTPUT_EXCHANGE_NAME")
OUTPUT_ROUTING_KEYS = (
    os.environ.get("OUTPUT_ROUTING_KEYS", "").split(",")
    if os.environ.get("OUTPUT_ROUTING_KEYS")
    else None
)

FILTER_AMOUNT = int(os.environ.get("FILTER_AMOUNT", "1"))

_max_amount_env = os.environ.get("MAX_AMOUNT")
MAX_AMOUNT = float(_max_amount_env) if _max_amount_env is not None else None
GE_DATE = os.environ.get("GE_DATE")
LE_DATE = os.environ.get("LE_DATE")
_pay_fmts_env = os.environ.get("PAY_FMTS")
PAY_FMTS = set(_pay_fmts_env.split(",")) if _pay_fmts_env is not None else None
USD_ONLY = bool(os.environ.get("USD_ONLY") == "True")
ADD_QUERY_ID = bool(os.environ.get("ADD_QUERY_ID") == "True")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


class Filter:
    def __init__(self):
        self.closed = False
        self.eof_seen: set[str] = set()
        self.batches: dict[str, list] = {}
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)

        if INPUT_EXCHANGE_NAME and INPUT_ROUTING_KEYS:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

        if OUTPUT_QUEUES:
            self.output_queues = [
                middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, q)
                for q in OUTPUT_QUEUES
            ]
        elif OUTPUT_EXCHANGE_NAME and OUTPUT_ROUTING_KEYS:
            self.output_queues = [
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
                )
            ]
        else:
            raise ValueError(
                "Must define OUTPUT_QUEUES or OUTPUT_EXCHANGE_NAME+OUTPUT_ROUTING_KEYS"
            )

    def _handle_sigterm(self, signum, frame):
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _get_eof_counter(self, fields):
        return FILTER_AMOUNT if len(fields) == 1 else int(fields[2])

    def _passes(self, tx) -> bool:
        return (
            (MAX_AMOUNT is None or tx.is_sent_amount_below(MAX_AMOUNT))
            and (
                (GE_DATE is None and LE_DATE is None)
                or tx.is_in_date_range(GE_DATE, LE_DATE)
            )
            and (PAY_FMTS is None or tx.has_any_payment_format(PAY_FMTS))
            and (not USD_ONLY or tx.is_usd())
        )

    def _send_output(self, message):
        for q in self.output_queues:
            q.send(message)

    def _flush_batch(self, client_id):
        batch = self.batches.pop(client_id, [])

        if not batch:
            return

        if ADD_QUERY_ID:
            self._send_output(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER, batch])
            )
        else:
            self._send_output(message_protocol.internal.serialize([client_id, batch]))

    def _on_eof(self, client_id, counter):
        if client_id not in self.eof_seen:
            self.eof_seen.add(client_id)
            logging.info(f"[QUERY {QUERY_NUMBER}] [FILTER] EOF received for client {client_id}")
            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                self._send_output(message_protocol.internal.serialize([client_id]))
                self.eof_seen.discard(client_id)
        else:
            self.input_queue.send(
                message_protocol.internal.serialize([client_id, "EOF", counter])
            )

    def _on_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if self._is_eof(fields):
                self._flush_batch(client_id)
                self._on_eof(client_id, self._get_eof_counter(fields))
                ack()
                return

            tx_list = [self._parse_transaction(tx_fields) for tx_fields in fields[1]]

            for tx in tx_list:
                if self._passes(tx):
                    self.batches.setdefault(client_id, []).append(tx.to_fields())

                    if len(self.batches[client_id]) >= BATCH_SIZE:
                        self._flush_batch(client_id)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting filter worker")
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self.input_queue.stop_consuming()
            self.input_queue.close()
            for q in self.output_queues:
                q.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    worker = Filter()
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"Error in filter: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
