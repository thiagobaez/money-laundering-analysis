import os
import logging
import signal
import threading

from common import middleware, message_protocol, transaction_item

ID = int(os.environ["ID"])
QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
NUM_INSTANCES = int(os.environ["NUM_INSTANCES"])
NUM_EXPECTED_EOFS = int(os.environ.get("NUM_EXPECTED_EOFS", "1"))
ADD_QUERY_ID = bool(os.environ.get("ADD_QUERY_ID") == "True")

INPUT_QUEUE = os.environ.get("INPUT_QUEUE")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE")

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

_max_amount_env = os.environ.get("MAX_AMOUNT")
MAX_AMOUNT = float(_max_amount_env) if _max_amount_env is not None else None
GE_DATE = os.environ.get("GE_DATE")
LE_DATE = os.environ.get("LE_DATE")
_pay_fmts_env = os.environ.get("PAY_FMTS")
PAY_FMTS = set(_pay_fmts_env.split(",")) if _pay_fmts_env is not None else None
USD_ONLY = bool(os.environ.get("USD_ONLY") == "True")


class Filter:
    def __init__(self):
        self.eof_count: dict[str, bool] = {}
        self._output_queue = None
        self._dedicated_consumer = None
        self._dedicated_thread = None

        if INPUT_EXCHANGE_NAME and INPUT_ROUTING_KEYS:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

        self._internal_producer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{FILTER_PREFIX}_{ID}"
        )

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

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

    def _forward_to_internal(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            for i in range(NUM_INSTANCES):
                q = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, f"{FILTER_PREFIX}_{i}"
                )
                q.send(message)
                q.close()
        else:
            self._internal_producer.send(message)
        ack()

    def _process_internal(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
                if self.eof_count[client_id] == NUM_EXPECTED_EOFS:
                    logging.info(
                        f"[QUERY {QUERY_NUMBER}] All EOFs received for client {client_id}"
                    )
                    self._output_queue.send(
                        message_protocol.internal.serialize([client_id])
                    )
                    del self.eof_count[client_id]
                ack()
                return

            tx = self._parse_transaction(fields[1])
            if self._passes(tx):
                if ADD_QUERY_ID:
                    out = message_protocol.internal.serialize(
                        [client_id, QUERY_NUMBER] + fields[1]
                    )
                else:
                    out = message
                self._output_queue.send(out)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def _run_dedicated_consumer(self):
        self._dedicated_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{FILTER_PREFIX}_{ID}"
        )
        if OUTPUT_EXCHANGE_NAME and OUTPUT_ROUTING_KEYS:
            self._output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
            )
        else:
            self._output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        self._dedicated_consumer.start_consuming(self._process_internal)
        self._output_queue.close()

    def start(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting filter worker")
        self._dedicated_thread = threading.Thread(
            target=self._run_dedicated_consumer, daemon=True
        )
        self._dedicated_thread.start()

        self.input_queue.start_consuming(self._forward_to_internal)

        if self._dedicated_consumer is not None:
            self._dedicated_consumer.stop_consuming()
        self._dedicated_thread.join()
        self._internal_producer.close()
        self.input_queue.close()
        if self._output_queue is not None:
            self._output_queue.close()

    def stop(self):
        self.input_queue.stop_consuming()


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    worker = Filter()
    signal.signal(signal.SIGTERM, lambda s, f: worker.stop())
    worker.start()
    logging.info(f"[QUERY {QUERY_NUMBER}] Filter worker stopped")


if __name__ == "__main__":
    main()
