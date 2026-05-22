import os
import logging
import signal
import threading

import requests

from common import middleware, message_protocol, transaction_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FRANKFURTER_BASE = os.environ.get("FRANKFURTER_BASE", "https://api.frankfurter.dev/v2")
BTC_USD_RATE = float(os.environ.get("BTC_USD_RATE", "20000.0"))
CONVERTER_PREFIX = os.environ["CONVERTER_PREFIX"]
NUM_INSTANCES = int(os.environ["NUM_INSTANCES"])
NUM_EXPECTED_EOFS = int(os.environ.get("NUM_EXPECTED_EOFS", "1"))

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


class Converter:
    def __init__(self):
        self._rate_lookup: dict[tuple[str, str], float] = {}
        self.eof_count: dict[str, int] = {}
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
            MOM_HOST, f"{CONVERTER_PREFIX}_{ID}"
        )

    def _get_rate(self, iso_code: str, date_iso: str) -> float:
        key = (date_iso, iso_code)
        if key in self._rate_lookup:
            return self._rate_lookup[key]
        url = f"{FRANKFURTER_BASE}/rates?base=USD&quotes={iso_code}&date={date_iso}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        rate = resp.json()[0]["rate"]
        self._rate_lookup[key] = rate
        return rate

    def _to_usd_fields(self, tx: transaction_item.TransactionItem) -> list:
        if not tx.is_usd():
            if tx.is_bitcoin():
                rate = BTC_USD_RATE
            else:
                rate = self._get_rate(
                    tx.get_receiving_currency_iso(), tx.get_date_iso()
                )
            tx.convert_to_usd(rate)
        return tx.to_fields()

    def _forward_to_internal(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            for i in range(NUM_INSTANCES):
                q = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, f"{CONVERTER_PREFIX}_{i}"
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
                        f"[CONVERTER] All EOFs received for client {client_id}"
                    )
                    self._output_queue.send(
                        message_protocol.internal.serialize([client_id])
                    )
                    del self.eof_count[client_id]
                ack()
                return

            tx_fields = fields[1]
            tx = transaction_item.TransactionItem(*tx_fields)
            converted_fields = self._to_usd_fields(tx)
            self._output_queue.send(
                message_protocol.internal.serialize([client_id, converted_fields])
            )
            ack()
        except Exception as e:
            logging.error(f"[CONVERTER] Error processing message: {e}")
            nack()

    def _run_dedicated_consumer(self):
        self._dedicated_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{CONVERTER_PREFIX}_{ID}"
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
        logging.info("[CONVERTER] Starting converter worker")
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
    worker = Converter()
    signal.signal(signal.SIGTERM, lambda s, f: worker.stop())
    worker.start()
    logging.info("[CONVERTER] stopped")


if __name__ == "__main__":
    main()
