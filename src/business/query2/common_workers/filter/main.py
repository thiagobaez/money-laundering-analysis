import os
import logging
import signal

from common import middleware, message_protocol, transaction_item

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]

INPUT_QUEUE = os.environ.get("INPUT_QUEUE")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE")

INPUT_EXCHANGE_NAME = os.environ.get("INPUT_EXCHANGE_NAME")
INPUT_ROUTING_KEYS = os.environ.get("INPUT_ROUTING_KEYS", "").split(",") if os.environ.get("INPUT_ROUTING_KEYS") else None

OUTPUT_EXCHANGE_NAME = os.environ.get("OUTPUT_EXCHANGE_NAME")
OUTPUT_ROUTING_KEYS = os.environ.get("OUTPUT_ROUTING_KEYS", "").split(",") if os.environ.get("OUTPUT_ROUTING_KEYS") else None

FILTER_AMOUNT = int(os.environ.get("FILTER_AMOUNT", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))

_max_amount_env = os.environ.get("MAX_AMOUNT")
MAX_AMOUNT = float(_max_amount_env) if _max_amount_env is not None else None
GE_DATE = os.environ.get("GE_DATE")
LE_DATE = os.environ.get("LE_DATE")
_pay_fmts_env = os.environ.get("PAY_FMTS")
PAY_FMTS = set(_pay_fmts_env.split(",")) if _pay_fmts_env is not None else None
USD_ONLY = bool(os.environ.get("USD_ONLY") == "True")
ADD_QUERY_ID = bool(os.environ.get("ADD_QUERY_ID") == "True")


class Filter:
    def __init__(self):
        self.closed = False
        self.eof_received_by_client = []
        self._batches = {}  # client_id -> list of rows
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)

        if(INPUT_EXCHANGE_NAME is None or INPUT_ROUTING_KEYS is None):
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        else:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS)

        if(OUTPUT_EXCHANGE_NAME is None or OUTPUT_ROUTING_KEYS is None):
            self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        else:
            self.output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, OUTPUT_EXCHANGE_NAME, OUTPUT_ROUTING_KEYS)


    def _handle_sigterm(self, signum, frame):
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _get_eof_counter(self, fields):
        return FILTER_AMOUNT if len(fields) == 1 else int(fields[2])

    def _flush_batch(self, client_id):
        rows = self._batches.pop(client_id, [])
        if not rows:
            return
        if ADD_QUERY_ID:
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER] + rows)
            )
        else:
            self.output_queue.send(
                message_protocol.internal.serialize([client_id] + rows)
            )

    def _on_eof(self, client_id, counter):
        self._flush_batch(client_id)
        if client_id not in self.eof_received_by_client:
            self.eof_received_by_client.append(client_id)
            logging.info(f"[QUERY {QUERY_NUMBER}] [FILTER] EOF received for client {client_id}")
            if counter > 1:
                self.input_queue.send(
                    message_protocol.internal.serialize([client_id, "EOF", counter - 1])
                )
            else:
                self.output_queue.send(message_protocol.internal.serialize([client_id]))
        else:
            self.input_queue.send(
                message_protocol.internal.serialize([client_id, "EOF", counter])
            )

    def _on_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if self._is_eof(fields):
                self._on_eof(client_id, self._get_eof_counter(fields))
                ack()
                return

            batch = self._batches.setdefault(client_id, [])
            for row in fields[1:]:
                tx = self._parse_transaction(row)

                passes = (
                    (MAX_AMOUNT is None or tx.is_sent_amount_below(MAX_AMOUNT))
                    and ((GE_DATE is None and LE_DATE is None) or tx.is_in_date_range(GE_DATE, LE_DATE))
                    and (PAY_FMTS is None or tx.has_any_payment_format(PAY_FMTS))
                    and (not USD_ONLY or tx.is_usd())
                )

                if passes:
                    batch.append(row)
                    if len(batch) >= BATCH_SIZE:
                        self._flush_batch(client_id)
                        batch = self._batches.setdefault(client_id, [])

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
