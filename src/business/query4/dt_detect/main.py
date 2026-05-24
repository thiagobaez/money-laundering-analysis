import os
import shutil
import logging
import signal

from common import middleware, message_protocol, transaction_item

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
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, INPUT_EXCHANGE_NAME, [INPUT_ROUTING_KEY])
        self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS)

    def _handle_sigterm(self, signum, frame):
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _client_dir(self, client_id):
        return os.path.join(DATA_DIR, str(client_id))

    def _on_eof_message(self, client_id):
        client_dir = self._client_dir(client_id)
        if os.path.exists(client_dir):
            for filename in os.listdir(client_dir):
                destination_account = filename[:-4]
                filepath = os.path.join(client_dir, filename)
                with open(filepath) as f:
                    origins = set(line.strip() for line in f if line.strip())
                if len(origins) >= MIN_ORIGINS:
                    message = message_protocol.internal.serialize(
                        [client_id, QUERY_NUMBER, destination_account] + list(origins))
                    self.output_queue.send(message)
            shutil.rmtree(client_dir)
        self.output_queue.send(message_protocol.internal.serialize([client_id, QUERY_NUMBER]))

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

            tx = self._parse_transaction(fields[2:])

            client_dir = self._client_dir(client_id)
            os.makedirs(client_dir, exist_ok=True)
            with open(os.path.join(client_dir, f"{tx._to_account}.csv"), "a") as f:
                f.write(tx._from_account + "\n")

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
    logging.basicConfig(level=logging.ERROR)
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
