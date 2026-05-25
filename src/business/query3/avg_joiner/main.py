import os
import csv
import logging
import signal
import threading

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
SECOND_PERIOD_QUEUE = os.environ["SECOND_PERIOD_QUEUE"]
AVG_EXCHANGE = os.environ["AVG_EXCHANGE"]
AVG_ROUTING_KEY = os.environ["AVG_ROUTING_KEY"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")
AVG_JOINER_AMOUNT = int(os.environ.get("AVG_JOINER_AMOUNT", "1"))
AVG_AMOUNT = int(os.environ.get("AVG_AMOUNT", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


class AvgJoiner:
    def __init__(self):
        self.eof_seen: set[str] = set()
        self.second_period_eof: set[str] = set()
        self.avg_eof: set[str] = set()
        self.avg_eof_counts: dict[str, int] = {}
        self.avg_results: dict[str, dict[str, float]] = {}
        self.avg_results_lock = threading.Lock()
        self.file_locks: dict[tuple, threading.Lock] = {}
        self.file_locks_lock = threading.Lock()
        self.output_batches: dict[str, list] = {}

        self.second_period_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, SECOND_PERIOD_QUEUE
        )
        self.avg_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AVG_EXCHANGE, [AVG_ROUTING_KEY]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _get_file_lock(self, client_id: str, payment_format: str) -> threading.Lock:
        key = (client_id, payment_format)
        with self.file_locks_lock:
            if key not in self.file_locks:
                self.file_locks[key] = threading.Lock()
            return self.file_locks[key]

    def _file_path(self, client_id: str, payment_format: str) -> str:
        client_dir = os.path.join(DATA_DIR, client_id)
        os.makedirs(client_dir, exist_ok=True)
        return os.path.join(client_dir, f"{payment_format}.csv")

    def _write_tx_to_disk(self, client_id: str, tx):
        path = self._file_path(client_id, tx.get_payment_format())
        file_lock = self._get_file_lock(client_id, tx.get_payment_format())
        with file_lock:
            with open(path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(tx.to_fields())

    def _flush_output_batch(self, client_id: str):
        batch = self.output_batches.pop(client_id, [])

        if batch:
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER, batch])
            )

    def _append_output_rows(self, client_id: str, rows: list):
        if not rows:
            return

        self.output_batches.setdefault(client_id, []).extend(rows)

        if len(self.output_batches[client_id]) >= BATCH_SIZE:
            self._flush_output_batch(client_id)

    def _flush_to_output(self, client_id: str, payment_format: str, avg: float):
        path = self._file_path(client_id, payment_format)
        file_lock = self._get_file_lock(client_id, payment_format)

        output_batch = []

        with file_lock:
            if not os.path.exists(path):
                return
            with open(path, "r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    try:
                        tx = transaction_item.TransactionItem(*row)
                        if tx.is_sent_amount_below(avg / 100):
                            output_batch.append(tx.to_fields())

                    except Exception as e:
                        logging.error(
                            f"[QUERY {QUERY_NUMBER}] Error processing tx from disk: {e}"
                        )
            os.remove(path)

        self._append_output_rows(client_id, output_batch)

    def _cleanup_client(self, client_id: str):
        client_dir = os.path.join(DATA_DIR, client_id)
        if os.path.exists(client_dir):
            for f in os.listdir(client_dir):
                os.remove(os.path.join(client_dir, f))
            os.rmdir(client_dir)
        with self.file_locks_lock:
            keys_to_remove = [k for k in self.file_locks if k[0] == client_id]
            for k in keys_to_remove:
                del self.file_locks[k]

    def _try_send_eof(self, client_id: str):
        if client_id in self.second_period_eof and client_id in self.avg_eof:
            logging.info(
                f"[QUERY {QUERY_NUMBER}] both EOFs received, sending EOF downstream client={client_id}"
            )
            self._flush_output_batch(client_id)
            self.output_queue.send(message_protocol.internal.serialize([client_id]))
            self.second_period_eof.discard(client_id)
            self.avg_eof.discard(client_id)

    def _on_second_period_message(self, message, ack, nack):
        logging.info(f"[QUERY {QUERY_NUMBER}] Received second period message")
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF"):
                counter = AVG_JOINER_AMOUNT if len(fields) == 1 else int(fields[2])
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] second_period EOF client={client_id} counter={counter}"
                )
                if client_id not in self.eof_seen:
                    self.eof_seen.add(client_id)
                    if counter > 1:
                        self.second_period_consumer.send(
                            message_protocol.internal.serialize(
                                [client_id, "EOF", counter - 1]
                            )
                        )
                    else:
                        self.second_period_eof.add(client_id)
                        self.eof_seen.discard(client_id)
                        self._try_send_eof(client_id)
                else:
                    if counter > 1:
                        self.second_period_consumer.send(
                            message_protocol.internal.serialize(
                                [client_id, "EOF", counter]
                            )
                        )
                    else:
                        self.second_period_eof.add(client_id)
                        self.eof_seen.discard(client_id)
                        self._try_send_eof(client_id)
                ack()
                return

            rows = fields[1]
            output_batch = []

            for row in rows:
                tx = transaction_item.TransactionItem(*row)
                payment_format = tx.get_payment_format()

                with self.avg_results_lock:
                    avg = self.avg_results.get(client_id, {}).get(payment_format)

                if avg is not None:
                    if tx.is_sent_amount_below(avg / 100):
                        output_batch.append(tx.to_fields())
                else:
                    self._write_tx_to_disk(client_id, tx)

            self._append_output_rows(client_id, output_batch)

            ack()
        except Exception as e:
            logging.error(
                f"[QUERY {QUERY_NUMBER}] Error processing second period message: {e}"
            )
            nack()

    def _on_avg_message(self, message, ack, nack):
        logging.info(f"[QUERY {QUERY_NUMBER}] Received avg message")
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                logging.info(f"[QUERY {QUERY_NUMBER}] avg EOF client={client_id}")
                self.avg_eof_counts[client_id] = (
                    self.avg_eof_counts.get(client_id, 0) + 1
                )
                if self.avg_eof_counts[client_id] < AVG_AMOUNT:
                    ack()
                    return
                del self.avg_eof_counts[client_id]
                with self.avg_results_lock:
                    self.avg_results.pop(client_id, {})
                self._cleanup_client(client_id)
                self.avg_eof.add(client_id)
                self._try_send_eof(client_id)
                ack()
                return

            avg_rows = fields[1]

            for payment_format, avg_value in avg_rows:
                avg = float(avg_value)

                with self.avg_results_lock:
                    if client_id not in self.avg_results:
                        self.avg_results[client_id] = {}

                    self.avg_results[client_id][payment_format] = avg

                self._flush_to_output(client_id, payment_format, avg)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing avg message: {e}")
            nack()

    def _run_avg_consumer(self):
        self.avg_consumer.start_consuming(self._on_avg_message)

    def start(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting avg_joiner worker")
        avg_thread = threading.Thread(target=self._run_avg_consumer, daemon=True)
        avg_thread.start()

        self.second_period_consumer.start_consuming(self._on_second_period_message)

        self.avg_consumer.stop_consuming()
        avg_thread.join()
        self.second_period_consumer.close()
        self.avg_consumer.close()
        self.output_queue.close()

    def stop(self):
        self.second_period_consumer.stop_consuming()


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    worker = AvgJoiner()
    signal.signal(signal.SIGTERM, lambda s, f: worker.stop())
    try:
        worker.start()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in avg_joiner: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main()
