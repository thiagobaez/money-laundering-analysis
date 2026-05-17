import csv
import io
import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MIN_AMOUNT = float(os.environ["MIN_AMOUNT"])


class Filter:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _parse_transaction(self, csv_line):
        row = next(csv.reader(io.StringIO(csv_line)))
        return transaction_item.TransactionItem(*row)

    def _on_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                logging.info(f"[worker {ID}] EOF received for client {client_id}")
                self.output_queue.send(message_protocol.internal.serialize([client_id]))
                ack()
                return

            tx = self._parse_transaction(fields[1])
            if tx.amount < MIN_AMOUNT:
                self.output_queue.send(message)

            ack()
        except Exception as e:
            logging.error(f"[worker {ID}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[worker {ID}] Starting filter worker (min_amount={MIN_AMOUNT})")
        self.input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self.closed = True
            self.input_queue.stop_consuming()
            self.output_queue.close()
            self.input_queue.close()
        except Exception as e:
            logging.error(f"[worker {ID}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = Filter()
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
