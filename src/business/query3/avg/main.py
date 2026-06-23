import os
import logging
import signal

from common import middleware, message_protocol, transaction_item, checkpoint

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUES = os.environ["OUTPUT_QUEUES"].split(",")


class Avg:
    def __init__(self, heartbeat=None):
        self.accum: dict[str, dict[str, list]] = {}
        self._last_msg_hash: str | None = None
        self._heartbeat = heartbeat

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.output_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, q)
            for q in OUTPUT_QUEUES
        ]
        self._load_checkpoint()

    def _load_checkpoint(self):
        state = checkpoint.load(DATA_DIR)
        if state is None:
            return
        self._last_msg_hash = state.get("last_msg_hash")
        self.accum = state.get("accum", {})
        logging.info(f"[QUERY {QUERY_NUMBER}] [AVG] Resumed from checkpoint")

    def _save_checkpoint(self):
        checkpoint.save(
            DATA_DIR,
            {
                "last_msg_hash": self._last_msg_hash,
                "accum": self.accum,
            },
        )

    def _parse_transaction(self, fields):
        return transaction_item.TransactionItem(*fields)

    def _is_eof(self, fields):
        return len(fields) == 1 or (len(fields) == 3 and fields[1] == "EOF")

    def _flush(self, client_id):
        client_data = self.accum.pop(client_id, {})

        batch = []

        for payment_format, (suma, count) in client_data.items():
            avg = suma / count
            batch.append([payment_format, avg])

        logging.info(
            f"[QUERY {QUERY_NUMBER}] [AVG] Flushing EOF for client {client_id}, "
            f"{len(batch)} payment formats, sending to {len(self.output_queues)} queues"
        )

        if batch:
            for q in self.output_queues:
                q.send(message_protocol.internal.serialize([client_id, batch]))

        for q in self.output_queues:
            q.send(message_protocol.internal.serialize([client_id]))

    def _on_message(self, message, ack, nack):
        try:
            h = checkpoint.msg_hash(message)
            if h == self._last_msg_hash:
                ack()
                return

            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if self._is_eof(fields):
                logging.info(
                    f"[QUERY {QUERY_NUMBER}] [AVG] EOF received for client {client_id}"
                )

                self._flush(client_id)
                self._last_msg_hash = h
                self._save_checkpoint()
                ack()
                return

            rows = fields[1]
            client_data = self.accum.setdefault(client_id, {})

            for row in rows:
                tx = self._parse_transaction(row)
                fmt = tx.get_payment_format()

                if fmt not in client_data:
                    client_data[fmt] = [0.0, 0]

                client_data[fmt][0] += tx.get_amount_paid()
                client_data[fmt][1] += 1

            self._last_msg_hash = h
            self._save_checkpoint()
            ack()

        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing message: {e}")
            nack()

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting avg worker")
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
    logging.basicConfig(level=logging.ERROR)
    from common.heartbeat import start_if_configured

    heartbeat = start_if_configured()
    worker = Avg(heartbeat)
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in avg: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
