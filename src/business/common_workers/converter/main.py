import os
import logging
import signal
import time

import requests

from common import middleware, message_protocol, transaction_item

MOM_HOST = os.environ["MOM_HOST"]
FRANKFURTER_BASE = os.environ.get("FRANKFURTER_BASE", "https://api.frankfurter.dev/v2")
BTC_USD_RATES_BY_DATE = {
    "2022-09-01": 19793.1,
    "2022-09-02": 199999.0,
    "2022-09-03": 19831.4,
    "2022-09-04": 19952.7,
    "2022-09-05": 20126.1,
}
CONVERTER_AMOUNT = int(os.environ.get("CONVERTER_AMOUNT", "1"))

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
        self.eof_seen: set[str] = set()

        if INPUT_EXCHANGE_NAME and INPUT_ROUTING_KEYS:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

        if OUTPUT_EXCHANGE_NAME and OUTPUT_ROUTING_KEYS:
            self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
            )
        else:
            self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

    def _get_rate(self, iso_code: str, date_iso: str) -> float:
        key = (date_iso, iso_code)
        if key in self._rate_lookup:
            return self._rate_lookup[key]
        url = f"{FRANKFURTER_BASE}/rates?base=USD&quotes={iso_code}&date={date_iso}"
        while True:
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                rate = resp.json()[0]["rate"]
                self._rate_lookup[key] = rate
                return rate
            except Exception as e:
                logging.warning("[CONVERTER] API error, retrying in 1s: %s", e)
                time.sleep(1)

    def _to_usd_fields(self, tx: transaction_item.TransactionItem) -> list:
        if not tx.is_usd():
            if tx.is_bitcoin():
                rate = BTC_USD_RATES_BY_DATE[tx.get_date_iso()]
            else:
                rate = self._get_rate(
                    tx.get_payment_currency_iso(),
                    tx.get_date_iso(),
                )
            tx.convert_to_usd(rate)
        return tx.to_fields()

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _get_eof_counter(self, fields):
        return CONVERTER_AMOUNT if len(fields) == 1 else int(fields[2])

    def _send_eof(self, client_id):
        self.output_queue.send(message_protocol.internal.serialize([client_id]))

    def _on_eof(self, client_id, counter):
        if client_id not in self.eof_seen:
            self.eof_seen.add(client_id)
            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                self._send_eof(client_id)
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
                self._on_eof(client_id, self._get_eof_counter(fields))
                ack()
                return

            rows = fields[1]
            converted_rows = []
            for row in rows:
                tx = transaction_item.TransactionItem(*row)
                converted_rows.append(self._to_usd_fields(tx))
            if converted_rows:
                self.output_queue.send(
                    message_protocol.internal.serialize([client_id, converted_rows])
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
            self.input_queue.stop_consuming()
            self.output_queue.close()
            self.input_queue.close()
        except Exception as e:
            logging.error(f"[CONVERTER] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    worker = Converter()
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
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
