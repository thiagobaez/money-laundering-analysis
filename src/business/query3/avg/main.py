import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
OUTPUT_ROUTING_KEYS = os.environ["OUTPUT_ROUTING_KEYS"].split(",")


class Avg:
    def __init__(self):
        # {client_id: {payment_format: [suma, count]}}
        self.accum: dict[str, dict[str, list]] = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, OUTPUT_ROUTING_KEYS
        )

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _flush(self, client_id):
        client_data = self.accum.pop(client_id, {})
        for payment_format, (suma, count) in client_data.items():
            avg = suma / count
            self.output_exchange.send(
                message_protocol.internal.serialize([client_id, payment_format, avg])
            )
        self.output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _on_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if self._is_eof(fields):
                self._flush(client_id)
                ack()
                return

            tx = self._parse_transaction(fields[1])
            client_data = self.accum.setdefault(client_id, {})
            fmt = tx.get_payment_format()
            if fmt not in client_data:
                client_data[fmt] = [0.0, 0]
            client_data[fmt][0] += tx.get_amount_received()
            client_data[fmt][1] += 1

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting avg worker")
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self.input_queue.stop_consuming()
            self.input_queue.close()
            self.output_exchange.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    worker = Avg()
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in avg: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
