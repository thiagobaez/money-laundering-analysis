import os
import shutil
import logging
import signal
import hashlib

from common import middleware, message_protocol

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
ORIGIN_ROUTING_KEY = os.environ["ORIGIN_ROUTING_KEY"]
OUTPUT_EXCHANGE_NAME = os.environ["OUTPUT_EXCHANGE_NAME"]
OUTPUT_ROUTING_KEYS = os.environ["OUTPUT_ROUTING_KEYS"].split(",")
MIN_DESTINATIONS = int(os.environ["MIN_DESTINATIONS"])

DATA_DIR = "/data"


class OgDetect:
    def __init__(self):
        self.closed = False
        self._logs = {}  # client_id -> file handle for the single log file
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_NAME, [ORIGIN_ROUTING_KEY]
        )
        self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
        )

    def _handle_sigterm(self, signum, frame):
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _get_hash_index_queue(self, account_id: str, cant_queues: int) -> int:
        digest = hashlib.md5(account_id.encode()).digest()
        return int.from_bytes(digest[:4], "big") % cant_queues

    def _client_dir(self, client_id):
        return os.path.join(DATA_DIR, str(client_id))

    def _get_log(self, client_id):
        if client_id not in self._logs:
            client_dir = self._client_dir(client_id)
            os.makedirs(client_dir, exist_ok=True)
            self._logs[client_id] = open(
                os.path.join(client_dir, "log.bin"), "ab", buffering=65536
            )
        return self._logs[client_id]

    def _on_eof_message(self, client_id):
        logging.info(
            f"[QUERY {QUERY_NUMBER}] [OG_DETECT] EOF received for client {client_id}"
        )
        if client_id in self._logs:
            self._logs.pop(client_id).close()

        log_path = os.path.join(self._client_dir(client_id), "log.bin")
        account_map = {}
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                for line in f:
                    parts = line.rstrip("\n").split("\t", 1)
                    if len(parts) == 2:
                        from_acc, to_acc = parts
                        if from_acc not in account_map:
                            account_map[from_acc] = set()
                        account_map[from_acc].add(to_acc)

        for from_acc, to_accs in account_map.items():
            if len(to_accs) >= MIN_DESTINATIONS:
                msg = message_protocol.internal.serialize(
                    [client_id, QUERY_NUMBER, from_acc] + list(to_accs)
                )
                idx = self._get_hash_index_queue(from_acc, len(OUTPUT_ROUTING_KEYS))
                self.output_queue.send(msg, OUTPUT_ROUTING_KEYS[idx])

        client_dir = self._client_dir(client_id)
        if os.path.exists(client_dir):
            shutil.rmtree(client_dir)

        eof = message_protocol.internal.serialize([client_id, QUERY_NUMBER])
        for routing_key in OUTPUT_ROUTING_KEYS:
            self.output_queue.send(eof, routing_key)

    def _on_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                self._on_eof_message(client_id)
                ack()
                return

            # fields = [client_id, query_number, batch]
            # each row = [timestamp, from_bank, from_account, to_bank, to_account, ...]
            log = self._get_log(client_id)
            for row in fields[2]:
                from_account = row[2]
                to_account = row[4]
                log.write(f"{from_account}\t{to_account}\n".encode())

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
            for fh in self._logs.values():
                fh.close()
            self._logs.clear()
            self.output_queue.close()
            self.input_queue.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = OgDetect()
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in og_detect: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
