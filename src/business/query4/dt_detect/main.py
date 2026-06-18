import os
import shutil
import logging
import signal

from common import middleware, message_protocol
from common.heartbeat import start_if_configured


QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE_NAME = os.environ["INPUT_EXCHANGE_NAME"]
INPUT_ROUTING_KEY = os.environ["INPUT_ROUTING_KEY"]
OUTPUT_EXCHANGE_NAME = os.environ["OUTPUT_EXCHANGE_NAME"]
OUTPUT_ROUTING_KEYS = os.environ["OUTPUT_ROUTING_KEYS"].split(",")
MIN_ORIGINS = int(os.environ["MIN_ORIGINS"])

DATA_DIR = "/data"


class DtDetect:
    def __init__(self):
        self.closed = False
        self._logs = {}
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE_NAME, [INPUT_ROUTING_KEY]
        )
        self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS
        )

    def _handle_sigterm(self, signum, frame):
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

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
        if client_id in self._logs:
            self._logs.pop(client_id).close()

        log_path = os.path.join(self._client_dir(client_id), "log.bin")
        account_map = {}
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                for line in f:
                    parts = line.rstrip("\n").split("\t", 1)
                    if len(parts) == 2:
                        to_acc, from_acc = parts
                        if to_acc not in account_map:
                            account_map[to_acc] = set()
                        account_map[to_acc].add(from_acc)

        for to_acc, from_accs in account_map.items():
            if len(from_accs) >= MIN_ORIGINS:
                msg = message_protocol.internal.serialize(
                    [client_id, QUERY_NUMBER, to_acc] + list(from_accs)
                )
                self.output_queue.send(msg)

        client_dir = self._client_dir(client_id)
        if os.path.exists(client_dir):
            shutil.rmtree(client_dir)

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, QUERY_NUMBER])
        )

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

            log = self._get_log(client_id)
            for row in fields[2]:
                from_account = row[2]
                to_account = row[4]
                log.write(f"{to_account}\t{from_account}\n".encode())

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
    start_if_configured()
    worker = DtDetect()
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in dt_detect: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
