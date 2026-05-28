import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
FIRST_PERIOD_QUEUES = os.environ["FIRST_PERIOD_QUEUES"].split(",")
SECOND_PERIOD_QUEUE = os.environ["SECOND_PERIOD_QUEUE"]
FIRST_PERIOD_GE = os.environ["FIRST_PERIOD_GE"]
FIRST_PERIOD_LE = os.environ["FIRST_PERIOD_LE"]
SECOND_PERIOD_GE = os.environ["SECOND_PERIOD_GE"]
SECOND_PERIOD_LE = os.environ["SECOND_PERIOD_LE"]
SPLIT_AMOUNT = int(os.environ.get("SPLIT_AMOUNT", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


class SplitDate:
    def __init__(self):
        self.eof_seen: set[str] = set()
        self.first_batches: dict[str, dict[int, list]] = {}
        self.second_batches: dict[str, list] = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.first_period_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, q)
            for q in FIRST_PERIOD_QUEUES
        ]
        self.second_period_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, SECOND_PERIOD_QUEUE
        )

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _get_eof_counter(self, fields):
        return SPLIT_AMOUNT if len(fields) == 1 else int(fields[2])

    def _get_first_queue_idx(self, payment_format: str) -> int:
        hash_value = 5381
        for c in payment_format:
            hash_value = ((hash_value << 5) + hash_value) + ord(c)
            hash_value &= 0xFFFFFFFF
        return hash_value % len(self.first_period_queues)

    def _flush_first_batch(self, client_id, idx):
        batch = self.first_batches.get(client_id, {}).pop(idx, [])

        if batch:
            logging.info(
                f"[QUERY {QUERY_NUMBER}] sending first_period batch client={client_id} queue_idx={idx} rows={len(batch)}"
            )
            self.first_period_queues[idx].send(
                message_protocol.internal.serialize([client_id, batch])
            )

    def _flush_second_batch(self, client_id):
        batch = self.second_batches.pop(client_id, [])
        if batch:
            logging.info(
                f"[QUERY {QUERY_NUMBER}] sending second_period batch client={client_id} rows={len(batch)}"
            )
            self.second_period_queue.send(
                message_protocol.internal.serialize([client_id, batch])
            )

    def _flush_all_batches(self, client_id):
        for idx in list(self.first_batches.get(client_id, {}).keys()):
            self._flush_first_batch(client_id, idx)
        self.first_batches.pop(client_id, None)
        self._flush_second_batch(client_id)

    def _on_eof(self, client_id, counter):
        # By the time this is called, batches have already been flushed in _on_message.
        eof = message_protocol.internal.serialize([client_id])
        logging.info(
            f"[QUERY {QUERY_NUMBER}] _on_eof called client={client_id} counter={counter}"
        )
        if client_id not in self.eof_seen:
            self.eof_seen.add(client_id)
            if counter > 1:
                # Pass EOF along with decremented counter for another worker to claim.
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                # Last worker to claim: send downstream EOFs.
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] sending EOF downstream client={client_id}"
                )
                for q in self.first_period_queues:
                    q.send(eof)
                self.second_period_queue.send(eof)
                self.eof_seen.discard(client_id)
        else:
            # This worker already claimed its slot; pass the message unchanged so
            # an unclaimed worker can process it.
            self.input_queue.send(
                message_protocol.internal.serialize([client_id, "EOF", counter])
            )

    def _on_message(self, message, ack, nack):
        logging.info(f"[QUERY {QUERY_NUMBER}] Received message")
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if self._is_eof(fields):
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] EOF received for client_id={client_id}"
                )
                # Flush pending batches before any EOF routing logic,
                # so every worker sends its share regardless of the counter value it sees.
                self._flush_all_batches(client_id)
                self._on_eof(client_id, self._get_eof_counter(fields))
                ack()
                return

            tx_list = [transaction_item.TransactionItem(*f) for f in fields[1]]

            for tx in tx_list:
                if tx.is_in_date_range(FIRST_PERIOD_GE, FIRST_PERIOD_LE):
                    idx = self._get_first_queue_idx(tx.get_payment_format())
                    self.first_batches.setdefault(client_id, {}).setdefault(
                        idx, []
                    ).append(tx.to_fields())
                    if len(self.first_batches[client_id][idx]) >= BATCH_SIZE:
                        self._flush_first_batch(client_id, idx)
                elif tx.is_in_date_range(SECOND_PERIOD_GE, SECOND_PERIOD_LE):
                    self.second_batches.setdefault(client_id, []).append(tx.to_fields())
                    if len(self.second_batches[client_id]) >= BATCH_SIZE:
                        self._flush_second_batch(client_id)

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
            for q in self.first_period_queues:
                q.close()
            self.second_period_queue.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
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
