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
        self.avg_eof_seen: dict[str, set] = {}
        self.avg_results: dict[str, dict[str, float]] = {}
        self.avg_results_lock = threading.Lock()
        self.spill_locks: dict[str, threading.Lock] = {}
        self.spill_locks_lock = threading.Lock()
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
        self.avg_eof_seen = {
            k: set(v) for k, v in state.get("avg_eof_seen", {}).items()
        }
        self.avg_results = state.get("avg_results", {})
        logging.info(f"[QUERY {QUERY_NUMBER}] [AVG_JOINER] Resumed from checkpoint")
        if os.path.isdir(DATA_DIR):
            for entry in os.listdir(DATA_DIR):
                client_dir = os.path.join(DATA_DIR, entry)
                if os.path.isdir(client_dir):
                    path = os.path.join(client_dir, "spill.csv")
                    if os.path.exists(path):
                        logging.info(
                            f"[QUERY {QUERY_NUMBER}] [AVG_JOINER] Recovering spill for client {entry}"
                        )
                        self._flush_spill_to_output(entry)

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
                    "avg_eof_seen": {k: list(v) for k, v in self.avg_eof_seen.items()},
                    "avg_results": self.avg_results,
                },
            )

    def _get_spill_lock(self, client_id: str) -> threading.Lock:
        with self.spill_locks_lock:
            if client_id not in self.spill_locks:
                self.spill_locks[client_id] = threading.Lock()
            return self.spill_locks[client_id]

    def _spill_path(self, client_id: str) -> str:
        client_dir = os.path.join(DATA_DIR, client_id)
        os.makedirs(client_dir, exist_ok=True)
        return os.path.join(client_dir, "spill.csv")

    def _spill_tx(self, client_id: str, tx):
        payment_format = tx.get_payment_format()

        with self.spill_lock:
            self.spill_batches.setdefault(client_id, {}).setdefault(
                payment_format, []
            ).append(tx.to_fields())

    def _flush_spill_to_disk(self, client_id: str):
        with self.spill_lock:
            client_batches = self.spill_batches.get(client_id, {})
            rows = [
                [payment_format] + fields
                for payment_format, fmt_rows in client_batches.items()
                for fields in fmt_rows
            ]

            if not rows:
                return

            self.spill_batches[client_id] = {}

            path = self._spill_path(client_id)
            tmp_path = path + ".tmp"
            existing_rows = []
            if os.path.exists(path):
                with open(path, "r", newline="") as f:
                    existing_rows = list(csv.reader(f))

            with open(tmp_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(existing_rows)
                writer.writerows(rows)
            os.replace(tmp_path, path)

    def _send_output(self, message):
        with self.output_lock:
            self.output_queue.send(message)

    def _append_output_rows(self, client_id: str, rows: list):
        if not rows:
            return

        self._send_output(
            message_protocol.internal.serialize([client_id, QUERY_NUMBER, rows])
        )

    def _cleanup_client(self, client_id: str):
        client_dir = os.path.join(DATA_DIR, client_id)
        if os.path.exists(client_dir):
            for f in os.listdir(client_dir):
                path = os.path.join(client_dir, f)
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            try:
                os.rmdir(client_dir)
            except OSError:
                pass

        with self.spill_lock:
            self.spill_batches.pop(client_id, None)

        with self.spill_locks_lock:
            self.spill_locks.pop(client_id, None)

    def _flush_spill_to_output(self, client_id: str, delete_all: bool = False):
        path = self._spill_path(client_id)
        spill_lock = self._get_spill_lock(client_id)

        with self.avg_results_lock:
            client_avgs = dict(self.avg_results.get(client_id, {}))

        output_batch = []

        with spill_lock:
            if not os.path.exists(path):
                return
            with open(path, "r", newline="") as f:
                rows = list(csv.reader(f))

            remaining_rows = []
            for row in rows:
                if not row:
                    continue
                payment_format = row[0]
                avg = client_avgs.get(payment_format)
                if avg is None:
                    if not delete_all:
                        remaining_rows.append(row)
                    continue
                try:
                    tx = transaction_item.TransactionItem(*row[1:])
                    if tx.is_sent_amount_below(avg / 100):
                        output_batch.append(tx.to_fields())
                except Exception as e:
                    logging.error(
                        f"[QUERY {QUERY_NUMBER}] Error processing spill row: {e}"
                    )

            if remaining_rows:
                tmp_path = path + ".tmp"
                with open(tmp_path, "w", newline="") as f:
                    csv.writer(f).writerows(remaining_rows)
                os.replace(tmp_path, path)
            else:
                os.remove(path)
                try:
                    os.rmdir(os.path.dirname(path))
                except OSError:
                    pass

        self._append_output_rows(client_id, output_batch)

    def _try_send_eof(self, client_id: str, msg_hash: str):
        with self.eof_coord_lock:
            if client_id not in self.sp_eof_done or client_id not in self.avg_eof:
                return

        self._flush_spill_to_output(client_id, delete_all=True)
        worker_id = os.environ.get("HOSTNAME", "unknown")
        self._send_output(
            message_protocol.internal.serialize(
                [client_id, QUERY_NUMBER, "EOF", worker_id]
            )
        )

        self._last_sp_hash = msg_hash
        self._save_checkpoint()

        with self.eof_coord_lock:
            self.avg_eof.discard(client_id)
            self.sp_eof_done.discard(client_id)

        with self.avg_results_lock:
            self.avg_results.pop(client_id, None)

        self._cleanup_client(client_id)
        self._save_checkpoint()

    def _on_second_period_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            h = checkpoint.msg_hash(message)
            if h == self._last_sp_hash:
                ack()
                return

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

                    self._try_send_eof(client_id, h)
                else:
                    self.second_period_consumer.send(
                        message_protocol.internal.serialize([client_id, "EOF", counter])
                    )

                self._save_checkpoint()
                ack()
                return

            rows = fields[1]

            for row in rows:
                tx = transaction_item.TransactionItem(*row)
                self._spill_tx(client_id, tx)

            self._flush_spill_to_disk(client_id)
            self._last_sp_hash = h
            self._save_checkpoint()
            self._flush_spill_to_output(client_id)
            ack()
        except Exception as e:
            logging.error(
                f"[QUERY {QUERY_NUMBER}] Error processing second period message: {e}"
            )
            nack()

    def _on_avg_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            h = checkpoint.msg_hash(message)
            if h == self._last_avg_hash:
                ack()
                return

            if len(fields) == 3 and fields[1] == "AVG_EOF":
                avg_id = fields[2]
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] avg EOF client={client_id} avg_id={avg_id}"
                )
                if client_id not in self.avg_eof_seen:
                    self.avg_eof_seen[client_id] = set()

                if avg_id in self.avg_eof_seen[client_id]:
                    ack()
                    return

                self.avg_eof_seen[client_id].add(avg_id)
                if len(self.avg_eof_seen[client_id]) < AVG_AMOUNT:
                    self._save_checkpoint()
                    ack()
                    return
                del self.avg_eof_seen[client_id]
                with self.eof_coord_lock:
                    self.avg_eof.add(client_id)
                self._try_send_eof(client_id, h)
                self._save_checkpoint()
                ack()
                return

            avg_rows = fields[1]

            for payment_format, avg_value in avg_rows:
                avg = float(avg_value)

                with self.avg_results_lock:
                    self.avg_results.setdefault(client_id, {})[payment_format] = avg
            # self._flush_spill_to_output(client_id)
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
