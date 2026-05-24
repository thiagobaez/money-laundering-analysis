import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
FIRST_PERIOD_EXCHANGE = os.environ["FIRST_PERIOD_EXCHANGE"]
FIRST_PERIOD_ROUTING_KEYS = os.environ["FIRST_PERIOD_ROUTING_KEYS"].split(",")
SECOND_PERIOD_QUEUE = os.environ["SECOND_PERIOD_QUEUE"]
FIRST_PERIOD_GE = os.environ["FIRST_PERIOD_GE"]
FIRST_PERIOD_LE = os.environ["FIRST_PERIOD_LE"]
SECOND_PERIOD_GE = os.environ["SECOND_PERIOD_GE"]
SECOND_PERIOD_LE = os.environ["SECOND_PERIOD_LE"]
SPLIT_AMOUNT = int(os.environ.get("SPLIT_AMOUNT", "1"))


class SplitDate:
    def __init__(self):
        self.eof_seen: set[str] = set()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.first_period_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FIRST_PERIOD_EXCHANGE, FIRST_PERIOD_ROUTING_KEYS
        )
        self.second_period_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, SECOND_PERIOD_QUEUE
        )

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _get_eof_counter(self, fields):
        return SPLIT_AMOUNT if len(fields) == 1 else int(fields[2])

    def _get_avg_routing_key(self, payment_format: str) -> str:
        hash_value = 5381
        for caracter in payment_format:
            hash_value = ((hash_value << 5) + hash_value) + ord(caracter)
            hash_value &= 0xFFFFFFFF
        idx = hash_value % len(FIRST_PERIOD_ROUTING_KEYS)
        return FIRST_PERIOD_ROUTING_KEYS[idx]

    def _on_eof(self, client_id, counter):
        eof = message_protocol.internal.serialize([client_id])
        logging.info(f"[QUERY {QUERY_NUMBER}] _on_eof called client={client_id} counter={counter}")
        if client_id not in self.eof_seen:
            self.eof_seen.add(client_id)
            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                for key in FIRST_PERIOD_ROUTING_KEYS:
                    self.first_period_exchange.send(eof, routing_key=key)
                self.second_period_queue.send(eof)
                self.eof_seen.discard(client_id)
        else:
            if counter > 1:
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

            tx = self._parse_transaction(fields[1])

            if tx.is_in_date_range(FIRST_PERIOD_GE, FIRST_PERIOD_LE):
                key = self._get_avg_routing_key(tx._payment_format)
                self.first_period_exchange.send(message, routing_key=key)
            elif tx.is_in_date_range(SECOND_PERIOD_GE, SECOND_PERIOD_LE):
                self.second_period_queue.send(message)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting split_date worker")
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self.input_queue.stop_consuming()
            self.input_queue.close()
            self.first_period_exchange.close()
            self.second_period_queue.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = SplitDate()
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in split_date: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
