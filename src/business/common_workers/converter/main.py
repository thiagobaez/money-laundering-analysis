import os
import logging
import signal

import requests

from common import middleware, message_protocol, transaction_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FRANKFURTER_BASE = os.environ.get("FRANKFURTER_BASE", "https://api.frankfurter.dev/v2")
BTC_USD_RATE = float(os.environ.get("BTC_USD_RATE", "20000.0"))

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
        self.closed = False
        self._rate_lookup: dict[tuple[str, str], float] = {}
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)

        if INPUT_EXCHANGE_NAME is None or INPUT_ROUTING_KEYS is None:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
        else:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
            )

        if OUTPUT_EXCHANGE_NAME is None or OUTPUT_ROUTING_KEYS is None:
            self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        else:
            self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
            )

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

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
                tx.convert_to_usd(rate)
            else:
                currency_iso = tx.get_receiving_currency_iso()
                date_iso = tx.get_date_iso()
                rate = self._get_rate(currency_iso, date_iso)
                tx.convert_to_usd(rate)

        return tx.to_fields()

    def _on_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                logging.info(f"[CONVERTER] EOF received for client {client_id}")
                self.output_queue.send(message_protocol.internal.serialize([client_id]))
                ack()
                return

            tx_fields = fields[1]
            tx = transaction_item.TransactionItem(*tx_fields)
            converted_fields = self._to_usd_fields(tx)

            self.output_queue.send(
                message_protocol.internal.serialize([client_id, converted_fields])
            )
            ack()
        except Exception as e:
            logging.error(f"[CONVERTER] Error processing message: {e}")
            nack()

    def run(self):
        logging.info("[CONVERTER] Starting converter worker")
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self.closed = True
            self.input_queue.stop_consuming()
            self.output_queue.close()
            self.input_queue.close()
        except Exception as e:
            logging.error(f"[CONVERTER] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    worker = Converter()
    try:
        worker.run()
    except Exception as e:
        logging.error(f"Error in converter: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
