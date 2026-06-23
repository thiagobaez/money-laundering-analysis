import os
import logging
import signal

from common import middleware, message_protocol, transaction_item, checkpoint

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")
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
    def __init__(self, heartbeat=None):
        self.eof_seen: set[str] = set()
        self.first_batches: dict[str, dict[int, list]] = {}
        self.second_batches: dict[str, list] = {}
        self._last_msg_hash: str | None = None
        self._heartbeat = heartbeat

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
        self._load_checkpoint()

    def _load_checkpoint(self):
        state = checkpoint.load(DATA_DIR)
        if state is None:
            return
        self._last_msg_hash = state.get("last_msg_hash")
        self.eof_seen = set(state.get("eof_seen", []))
        self.first_batches = {
            client_id: {int(k): v for k, v in per_idx.items()}
            for client_id, per_idx in state.get("first_batches", {}).items()
        }
        self.second_batches = state.get("second_batches", {})
        logging.info(f"[QUERY {QUERY_NUMBER}] [SPLIT_DATE] Resumed from checkpoint")

    def _save_checkpoint(self):
        checkpoint.save(
            DATA_DIR,
            {
                "last_msg_hash": self._last_msg_hash,
                "eof_seen": list(self.eof_seen),
                "first_batches": {
                    client_id: {str(k): v for k, v in per_idx.items()}
                    for client_id, per_idx in self.first_batches.items()
                },
                "second_batches": self.second_batches,
            },
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
            self.first_period_queues[idx].send(
                message_protocol.internal.serialize([client_id, batch])
            )

    def _flush_second_batch(self, client_id):
        batch = self.second_batches.pop(client_id, [])
        if batch:
            self.second_period_queue.send(
                message_protocol.internal.serialize([client_id, batch])
            )

    def _flush_all_batches(self, client_id):
        for idx in list(self.first_batches.get(client_id, {}).keys()):
            self._flush_first_batch(client_id, idx)
        self.first_batches.pop(client_id, None)
        self._flush_second_batch(client_id)

    def _on_eof(self, client_id, counter, msg_hash):
        eof = message_protocol.internal.serialize([client_id])
        logging.info(
            f"[QUERY {QUERY_NUMBER}] _on_eof called client={client_id} counter={counter}"
        )
        if client_id not in self.eof_seen:
            self.eof_seen.add(client_id)
            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] sending EOF downstream client={client_id}"
                )
                for q in self.first_period_queues:
                    q.send(eof)
                self.second_period_queue.send(eof)
                self._last_msg_hash = msg_hash
                self._save_checkpoint()
                self.eof_seen.discard(client_id)
        else:
            self.input_queue.send(
                message_protocol.internal.serialize([client_id, "EOF", counter])
            )

    def _on_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            h = checkpoint.msg_hash(message)
            if h == self._last_msg_hash:
                ack()
                return

            if self._is_eof(fields):
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] EOF received for client_id={client_id}"
                )
                self._flush_all_batches(client_id)
                self._on_eof(client_id, self._get_eof_counter(fields), h)
                self._save_checkpoint()
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

            self._last_msg_hash = h
            self._save_checkpoint()
            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting split_date worker")
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        if self._heartbeat:
            self._heartbeat.stop()
            self._heartbeat = None
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
    from common.heartbeat import start_if_configured

    heartbeat = start_if_configured()
    worker = SplitDate(heartbeat)
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
