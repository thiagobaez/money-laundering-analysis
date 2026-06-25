import os
import csv
import logging
import signal

from common import middleware, message_protocol, transaction_item, checkpoint

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")

INPUT_QUEUE = os.environ.get("INPUT_QUEUE")
OUTPUT_QUEUES = (
    os.environ.get("OUTPUT_QUEUES", "").split(",")
    if os.environ.get("OUTPUT_QUEUES")
    else None
)

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

FILTER_AMOUNT = int(os.environ.get("FILTER_AMOUNT", "1"))

_max_amount_env = os.environ.get("MAX_AMOUNT")
MAX_AMOUNT = float(_max_amount_env) if _max_amount_env is not None else None
GE_DATE = os.environ.get("GE_DATE")
LE_DATE = os.environ.get("LE_DATE")
_pay_fmts_env = os.environ.get("PAY_FMTS")
PAY_FMTS = set(_pay_fmts_env.split(",")) if _pay_fmts_env is not None else None
USD_ONLY = bool(os.environ.get("USD_ONLY") == "True")
ADD_QUERY_ID = bool(os.environ.get("ADD_QUERY_ID") == "True")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


class Filter:
    def __init__(self, heartbeat=None):
        self.closed = False
        self.eof_seen: set[str] = set()
        self.batches: dict[str, list] = {}
        self._last_msg_hash: str | None = None
        self._heartbeat = heartbeat
        self._disk_counts: dict[str, int] = {}

        if INPUT_EXCHANGE_NAME and INPUT_ROUTING_KEYS:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

        if OUTPUT_QUEUES:
            self.output_queues = [
                middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, q)
                for q in OUTPUT_QUEUES
            ]
        elif OUTPUT_EXCHANGE_NAME and OUTPUT_ROUTING_KEYS:
            self.output_queues = [
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
                )
            ]
        else:
            raise ValueError(
                "Must define OUTPUT_QUEUES or OUTPUT_EXCHANGE_NAME+OUTPUT_ROUTING_KEYS"
            )
        self._load_checkpoint()

    def _load_checkpoint(self):
        state = checkpoint.load(DATA_DIR)
        if state is not None:
            self._last_msg_hash = state.get("last_msg_hash")
            self.eof_seen = set(state.get("eof_seen", []))
            logging.info(f"[QUERY {QUERY_NUMBER}] [FILTER] Resumed from checkpoint")
        if os.path.isdir(DATA_DIR):
            for entry in os.listdir(DATA_DIR):
                client_dir = os.path.join(DATA_DIR, entry)
                if os.path.isdir(client_dir):
                    path = self._file_path(entry)
                    if os.path.exists(path):
                        logging.info(
                            f"[QUERY {QUERY_NUMBER}] [FILTER] Recovering spilled rows for client {entry}"
                        )
                        self._send_disk_rows(entry)

    def _save_checkpoint(self):
        checkpoint.save(
            DATA_DIR,
            {
                "last_msg_hash": self._last_msg_hash,
                "eof_seen": list(self.eof_seen),
            },
        )

    def _file_path(self, client_id):
        return os.path.join(DATA_DIR, client_id, "rows.csv")

    def _flush_batch_to_disk(self, client_id):
        rows = self.batches.pop(client_id, [])
        if not rows:
            return

        path = self._file_path(client_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
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

        self._disk_counts[client_id] = self._disk_counts.get(client_id, 0) + len(rows)

    def _send_disk_rows(self, client_id):
        path = self._file_path(client_id)
        if not os.path.exists(path):
            return

        with open(path, "r", newline="") as f:
            rows = list(csv.reader(f))
        self._send_batch(client_id, rows)
        os.remove(path)
        try:
            os.rmdir(os.path.dirname(path))
        except OSError:
            pass
        self._disk_counts[client_id] = 0

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _get_eof_counter(self, fields):
        return FILTER_AMOUNT if len(fields) == 1 else int(fields[2])

    def _passes(self, tx) -> bool:
        return (
            (MAX_AMOUNT is None or tx.is_sent_amount_below(MAX_AMOUNT))
            and (
                (GE_DATE is None and LE_DATE is None)
                or tx.is_in_date_range(GE_DATE, LE_DATE)
            )
            and (PAY_FMTS is None or tx.has_any_payment_format(PAY_FMTS))
            and (not USD_ONLY or tx.is_usd())
        )

    def _send_output(self, message):
        for q in self.output_queues:
            q.send(message)

    def _send_batch(self, client_id, batch):
        if not batch:
            return

        if ADD_QUERY_ID:
            self._send_output(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER, batch])
            )
        else:
            self._send_output(message_protocol.internal.serialize([client_id, batch]))

    def _on_eof(self, client_id, counter, msg_hash):
        self._flush_batch_to_disk(client_id)
        self._send_disk_rows(client_id)

        if client_id not in self.eof_seen:
            self.eof_seen.add(client_id)

            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] [FILTER] Sending EOF downstream for client {client_id} (first time, counter=1)"
                )
                self._send_output(message_protocol.internal.serialize([client_id]))
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
                self._on_eof(client_id, self._get_eof_counter(fields), h)
                self._save_checkpoint()
                ack()
                return

            tx_list = [self._parse_transaction(tx_fields) for tx_fields in fields[1]]

            for tx in tx_list:
                if self._passes(tx):
                    self.batches.setdefault(client_id, []).append(tx.to_fields())

            self._flush_batch_to_disk(client_id)
            self._last_msg_hash = h
            self._save_checkpoint()

            if self._disk_counts.get(client_id, 0) >= BATCH_SIZE:
                self._send_disk_rows(client_id)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        if self._heartbeat:
            self._heartbeat.stop()
            self._heartbeat = None
        try:
            self.input_queue.stop_consuming()
            self.input_queue.close()
            for q in self.output_queues:
                q.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    from common.heartbeat import start_if_configured

    heartbeat = start_if_configured()
    worker = Filter(heartbeat)
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"Error in filter: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
