import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

ID = int(os.environ["ID"])
QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]

_max_amount_env = os.environ.get("MAX_AMOUNT")
MAX_AMOUNT = float(_max_amount_env) if _max_amount_env is not None else None
GE_DATE = os.environ.get("GE_DATE")
LE_DATE = os.environ.get("LE_DATE")
_pay_fmts_env = os.environ.get("PAY_FMTS")
PAY_FMTS = set(_pay_fmts_env.split(",")) if _pay_fmts_env is not None else None


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

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

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
                self.output_queue.send(message_protocol.internal.serialize([client_id]))
                ack()
                return

            tx = self._parse_transaction(fields[1])

            passes = (
                (MAX_AMOUNT is None or tx.is_sent_amount_below(MAX_AMOUNT))
                and ((GE_DATE is None and LE_DATE is None) or tx.is_in_date_range(GE_DATE, LE_DATE))
                and (PAY_FMTS is None or tx.has_any_payment_format(PAY_FMTS))
            )

            if passes:
                self.output_queue.send(
                    message_protocol.internal.serialize(
                        [client_id, QUERY_NUMBER] + fields[1]
                    )
                )

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting filter worker")
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
