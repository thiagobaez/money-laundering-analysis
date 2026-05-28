import os
import logging
import signal
import hashlib

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
ORIGIN_ROUTING_KEYS = os.environ["ORIGIN_ROUTING_KEYS"].split(",")
DESTINATION_ROUTING_KEYS = os.environ["DESTINATION_ROUTING_KEYS"].split(",")
SPLIT_AMOUNT = int(os.environ["SPLIT_AMOUNT"])
BATCH_SIZE = int(os.environ["BATCH_SIZE"])


class Split:
    def __init__(self):
        self.closed = False
        self.eof_received_by_client = []
        self._origin_batches = {}  # (client_id, routing_key) -> list of rows
        self._dest_batches = {}  # (client_id, routing_key) -> list of rows
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_NAME, ORIGIN_ROUTING_KEYS + DESTINATION_ROUTING_KEYS
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
        return SPLIT_AMOUNT if len(fields) == 1 else int(fields[2])

    def _get_hash_index_queue(self, account_id: str, cant_queues: int) -> int:
        digest = hashlib.md5(account_id.encode()).digest()
        return int.from_bytes(digest[:4], "big") % cant_queues

    def _flush_origin_batch(self, client_id, key):
        rows = self._origin_batches.pop((client_id, key), [])
        if rows:
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER] + rows),
                key,
            )

    def _flush_dest_batch(self, client_id, key):
        rows = self._dest_batches.pop((client_id, key), [])
        if rows:
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER] + rows),
                key,
            )

    def _flush_all_batches(self, client_id):
        for key in ORIGIN_ROUTING_KEYS:
            self._flush_origin_batch(client_id, key)
        for key in DESTINATION_ROUTING_KEYS:
            self._flush_dest_batch(client_id, key)

    def _on_eof(self, client_id, counter):
        self._flush_all_batches(client_id)
        if client_id not in self.eof_received_by_client:
            self.eof_received_by_client.append(client_id)
            logging.info(
                f"[QUERY {QUERY_NUMBER}] [SPLIT] EOF received for client {client_id}"
            )
            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                self.output_queue.send(message_protocol.internal.serialize([client_id]))
        else:
            self.input_queue.send(
                message_protocol.internal.serialize([client_id, "EOF", counter])
            )
            self.eof_received_by_client.remove(client_id)

    def _on_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if self._is_eof(fields):
                self._on_eof(client_id, self._get_eof_counter(fields))
                ack()
                return

            # fields = [client_id, query_number, row1, row2, ...]
            for row in fields[2:]:
                tx = self._parse_transaction(row)

                origin_key = ORIGIN_ROUTING_KEYS[
                    self._get_hash_index_queue(
                        tx.get_from_account(), len(ORIGIN_ROUTING_KEYS)
                    )
                ]
                dest_key = DESTINATION_ROUTING_KEYS[
                    self._get_hash_index_queue(
                        tx.get_to_account(), len(DESTINATION_ROUTING_KEYS)
                    )
                ]

                origin_batch = self._origin_batches.setdefault(
                    (client_id, origin_key), []
                )
                origin_batch.append(row)
                if len(origin_batch) >= BATCH_SIZE:
                    self._flush_origin_batch(client_id, origin_key)

                dest_batch = self._dest_batches.setdefault((client_id, dest_key), [])
                dest_batch.append(row)
                if len(dest_batch) >= BATCH_SIZE:
                    self._flush_dest_batch(client_id, dest_key)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self.closed = True
            self.input_queue.stop_consuming()
            self.output_queue.close()
            self.input_queue.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = Split()
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in split: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
