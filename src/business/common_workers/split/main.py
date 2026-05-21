import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
ORIGIN_ROUTING_KEYS = os.environ["ORIGIN_ROUTING_KEYS"].split(",")
DESTINATION_ROUTING_KEYS = os.environ["DESTINATION_ROUTING_KEYS"].split(",")


class Split:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, 
        EXCHANGE_NAME, ORIGIN_ROUTING_KEYS + DESTINATION_ROUTING_KEYS)


    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)
            

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)
    
    
    def _send_eof_to_all(self, client_id):
        self.output_queue.send(message_protocol.internal.serialize([client_id]))

    
    def _get_hash_index_queue(account_id: str, cant_queues: int) -> int:
        hash_value = 5381 
        for caracter in account_id:
            hash_value = ((hash_value << 5) + hash_value) + ord(caracter)
            hash_value &= 0xFFFFFFFF
        return hash_value % cant_queues
    

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
                self._send_eof_to_all(client_id)
                ack()
                return

            tx = self._parse_transaction(fields[2:])

            i_origin = self._get_hash_index_queue(tx.get_from_account(), len(ORIGIN_ROUTING_KEYS))
            i_destination = self._get_hash_index_queue(tx.get_to_account(), len(DESTINATION_ROUTING_KEYS))

            logging.info(
                f"[QUERY {QUERY_NUMBER}] Routing transaction {tx._amount_paid} to origin queue {ORIGIN_ROUTING_KEYS[i_origin]} and destination queue {DESTINATION_ROUTING_KEYS[i_destination]}"
            )
            self.output_queue.send(message, ORIGIN_ROUTING_KEYS[i_origin])
            self.output_queue.send(message, DESTINATION_ROUTING_KEYS[i_destination])

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting split worker")
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
    worker = Split()
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in split: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
