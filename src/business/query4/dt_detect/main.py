import os
import logging
import signal

from common import middleware, message_protocol, transaction_item
from collections import defaultdict

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
ORIGIN_ROUTING_KEY = os.environ["ORIGIN_ROUTING_KEY"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MIN_ORIGINS = int(os.environ["MIN_ORIGINS"])

class DtDetect:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, EXCHANGE_NAME, [ORIGIN_ROUTING_KEY])
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.destinations_accounts = defaultdict(set)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)
            

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)
    
    
    def _on_eof_message(self, client_id):
        for destination_account, origins_accounts in self.destinations_accounts.items():
            if len(origins_accounts) >= MIN_ORIGINS:
                self.output_queue.send(message_protocol.internal.serialize(
                    [client_id, QUERY_NUMBER, destination_account] + list(origins_accounts)))
        self.output_queue.send(message_protocol.internal.serialize([client_id, QUERY_NUMBER]))

    def _on_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 1:
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] EOF received for client {client_id}"
                )
                self._on_eof_message(client_id)
                ack()
                return

            tx = self._parse_transaction(fields[2:])

            logging.info(f"[QUERY {QUERY_NUMBER}] Received transaction from account with amount {tx._amount_paid}")
            self.destinations_accounts[tx._to_account].add(tx._from_account)

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting dt_detect worker")
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
    logging.basicConfig(level=logging.INFO)
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
