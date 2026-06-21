import os
import csv
import logging
import signal
import threading

from common import middleware, message_protocol, transaction_item, checkpoint

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")
SECOND_PERIOD_QUEUE = os.environ["SECOND_PERIOD_QUEUE"]
AVG_QUEUE = os.environ["AVG_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AVG_JOINER_AMOUNT = int(os.environ.get("AVG_JOINER_AMOUNT", "1"))
AVG_AMOUNT = int(os.environ.get("AVG_AMOUNT", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


class AvgJoiner:
    def __init__(self, heartbeat=None):
        self._heartbeat = heartbeat
        self.eof_seen: set[str] = set()
        self.sp_eof_done: set[str] = set()
        self.avg_eof: set[str] = set()
        self.avg_eof_counts: dict[str, int] = {}
        self.avg_results: dict[str, dict[str, float]] = {}
        self.avg_results_lock = threading.Lock()
        self.file_locks: dict[tuple, threading.Lock] = {}
        self.file_locks_lock = threading.Lock()
        self.output_batches: dict[str, list] = {}
        self.spill_batches: dict[str, dict[str, list]] = {}
        self.spill_lock = threading.Lock()
        self.eof_coord_lock = threading.Lock()

        self.second_period_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, SECOND_PERIOD_QUEUE
        )
        self.avg_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, AVG_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.output_lock = threading.Lock()
        self._checkpoint_lock = threading.Lock()
        self._last_sp_hash: str | None = None
        self._last_avg_hash: str | None = None
        self._load_checkpoint()

    def _load_checkpoint(self):
        state = checkpoint.load(DATA_DIR)
        if state is None:
            return
        self._last_sp_hash = state.get("last_sp_hash")
        self._last_avg_hash = state.get("last_avg_hash")
        self.eof_seen = set(state.get("eof_seen", []))
        self.sp_eof_done = set(state.get("sp_eof_done", []))
        self.avg_eof = set(state.get("avg_eof", []))
        self.avg_eof_counts = state.get("avg_eof_counts", {})
        self.avg_results = state.get("avg_results", {})
        logging.info(f"[QUERY {QUERY_NUMBER}] [AVG_JOINER] Resumed from checkpoint")

    def _save_checkpoint(self):
        with self._checkpoint_lock:
            checkpoint.save(
                DATA_DIR,
                {
                    "last_sp_hash": self._last_sp_hash,
                    "last_avg_hash": self._last_avg_hash,
                    "eof_seen": list(self.eof_seen),
                    "sp_eof_done": list(self.sp_eof_done),
                    "avg_eof": list(self.avg_eof),
                    "avg_eof_counts": self.avg_eof_counts,
                    "avg_results": self.avg_results,
                },
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

    def _spill_tx(self, client_id: str, tx):
        payment_format = tx.get_payment_format()

        with self.spill_lock:
            self.spill_batches.setdefault(client_id, {}).setdefault(
                payment_format, []
            ).append(tx.to_fields())

            should_flush = (
                len(self.spill_batches[client_id][payment_format]) >= BATCH_SIZE
            )

        if should_flush:
            self._flush_spill_batch(client_id, payment_format)

    def _flush_spill_batch(self, client_id: str, payment_format: str):
        with self.spill_lock:
            rows = self.spill_batches.get(client_id, {}).get(payment_format, [])

            if not rows:
                return

            self.spill_batches[client_id][payment_format] = []

        path = self._file_path(client_id, payment_format)
        file_lock = self._get_file_lock(client_id, payment_format)

        with file_lock:
            with open(path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(rows)

    def _send_output(self, message):
        with self.output_lock:
            self.output_queue.send(message)

    def _flush_output_batch(self, client_id: str):
        with self.output_lock:
            batch = self.output_batches.pop(client_id, [])

            if batch:
                self.output_queue.send(
                    message_protocol.internal.serialize(
                        [client_id, QUERY_NUMBER, batch]
                    )
                )

    def _append_output_rows(self, client_id: str, rows: list):
        if not rows:
            return

        with self.output_lock:
            self.output_batches.setdefault(client_id, []).extend(rows)

            if len(self.output_batches[client_id]) >= BATCH_SIZE:
                batch = self.output_batches.pop(client_id, [])

                self.output_queue.send(
                    message_protocol.internal.serialize(
                        [client_id, QUERY_NUMBER, batch]
                    )
                )

    def _flush_to_output(self, client_id: str, payment_format: str, avg: float):
        self._flush_spill_batch(client_id, payment_format)
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
                path = os.path.join(client_dir, f)
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass

        with self.spill_lock:
            self.spill_batches.pop(client_id, None)

        with self.file_locks_lock:
            keys_to_remove = [k for k in self.file_locks if k[0] == client_id]
            for k in keys_to_remove:
                del self.file_locks[k]

    def _flush_all_spill_to_output(self, client_id: str):
        with self.avg_results_lock:
            client_avgs = dict(self.avg_results.get(client_id, {}))

        with self.spill_lock:
            pending_formats = set(self.spill_batches.get(client_id, {}).keys())

        for payment_format in pending_formats:
            if payment_format in client_avgs:
                self._flush_to_output(
                    client_id, payment_format, client_avgs[payment_format]
                )

        with self.avg_results_lock:
            client_avgs = dict(self.avg_results.get(client_id, {}))

        for payment_format, avg in client_avgs.items():
            self._flush_to_output(client_id, payment_format, avg)

    def _try_send_eof(self, client_id: str):
        with self.eof_coord_lock:
            if client_id not in self.sp_eof_done or client_id not in self.avg_eof:
                return
            self.sp_eof_done.discard(client_id)
            self.avg_eof.discard(client_id)

        self._flush_all_spill_to_output(client_id)
        self._flush_output_batch(client_id)
        self._send_output(message_protocol.internal.serialize([client_id]))
        with self.avg_results_lock:
            self.avg_results.pop(client_id, None)
        self._cleanup_client(client_id)

    def _on_second_period_message(self, message, ack, nack):
        logging.info(f"[QUERY {QUERY_NUMBER}] Received second period message")
        try:
            h = checkpoint.msg_hash(message)
            if h == self._last_sp_hash:
                ack()
                return

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

                    with self.eof_coord_lock:
                        self.sp_eof_done.add(client_id)

                    self._try_send_eof(client_id)
                else:
                    self.second_period_consumer.send(
                        message_protocol.internal.serialize([client_id, "EOF", counter])
                    )
                self._last_sp_hash = h
                self._save_checkpoint()
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
                    self._spill_tx(client_id, tx)

            self._append_output_rows(client_id, output_batch)

            self._last_sp_hash = h
            self._save_checkpoint()
            ack()
        except Exception as e:
            logging.error(
                f"[QUERY {QUERY_NUMBER}] Error processing second period message: {e}"
            )
            nack()

    def _on_avg_message(self, message, ack, nack):
        logging.info(f"[QUERY {QUERY_NUMBER}] Received avg message")
        try:
            h = checkpoint.msg_hash(message)
            if h == self._last_avg_hash:
                ack()
                return

            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                logging.info(f"[QUERY {QUERY_NUMBER}] avg EOF client={client_id}")
                self.avg_eof_counts[client_id] = (
                    self.avg_eof_counts.get(client_id, 0) + 1
                )
                if self.avg_eof_counts[client_id] < AVG_AMOUNT:
                    self._last_avg_hash = h
                    self._save_checkpoint()
                    ack()
                    return
                del self.avg_eof_counts[client_id]
                with self.eof_coord_lock:
                    self.avg_eof.add(client_id)
                self._try_send_eof(client_id)
                self._last_avg_hash = h
                self._save_checkpoint()
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

            self._last_avg_hash = h
            self._save_checkpoint()
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

        try:
            self.second_period_consumer.start_consuming(self._on_second_period_message)
            self.avg_consumer.stop_consuming()
            avg_thread.join()
            self.second_period_consumer.close()
            self.avg_consumer.close()
            self.output_queue.close()
        finally:
            if self._heartbeat:
                self._heartbeat.stop()
                self._heartbeat = None

    def stop(self):
        self.second_period_consumer.stop_consuming()


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.ERROR)
    from common.heartbeat import start_if_configured

    heartbeat = start_if_configured()
    worker = AvgJoiner(heartbeat)
    signal.signal(signal.SIGTERM, lambda s, f: worker.stop())
    try:
        worker.start()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in avg_joiner: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main()
