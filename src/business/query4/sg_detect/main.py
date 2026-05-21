import os
import logging
import signal

from common import middleware, message_protocol
from collections import defaultdict

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
ORIGINS_QUEUE = os.environ["ORIGINS_QUEUE"]
DESTINATIONS_QUEUE = os.environ["DESTINATIONS_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MIN_COMMON = int(os.environ["MIN_COMMON"])
NUM_OG_WORKERS = int(os.environ.get("NUM_OG_WORKERS", "1"))
NUM_DT_WORKERS = int(os.environ.get("NUM_DT_WORKERS", "1"))


class SgDetect:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.origins_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, ORIGINS_QUEUE)
        self.destinations_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, DESTINATIONS_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        # A[origin] = set of destinations  (from og_detect)
        self.A = defaultdict(set)
        # B[destination] = set of origins  (from dt_detect)
        self.B = defaultdict(set)
        self.client_id = None
        self.origins_eofs = 0
        self.destinations_eofs = 0

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _on_origins_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 2:
                self.origins_eofs += 1
                logging.info(f"[QUERY {QUERY_NUMBER}] EOF {self.origins_eofs}/{NUM_OG_WORKERS} from origins queue for client {client_id}")
                self.client_id = client_id
                if self.origins_eofs >= NUM_OG_WORKERS:
                    self.origins_queue.stop_consuming()
                ack()
                return

            # fields: [client_id, query_number, origin_account, dest1, dest2, ...]
            origin = fields[2]
            self.A[origin].update(fields[3:])
            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing origins message: {e}")
            nack()

    def _on_destinations_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]

            if len(fields) == 2:
                self.destinations_eofs += 1
                logging.info(f"[QUERY {QUERY_NUMBER}] EOF {self.destinations_eofs}/{NUM_DT_WORKERS} from destinations queue for client {client_id}")
                if self.destinations_eofs >= NUM_DT_WORKERS:
                    self.destinations_queue.stop_consuming()
                ack()
                return

            # fields: [client_id, query_number, destination_account, origin1, origin2, ...]
            dest = fields[2]
            self.B[dest].update(fields[3:])
            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing destinations message: {e}")
            nack()

    def _emit_results(self):
        for origin, dests_of_origin in self.A.items():
            for dest, origins_of_dest in self.B.items():
                intersection = dests_of_origin & origins_of_dest
                if len(intersection) >= MIN_COMMON:
                    self.output_queue.send(message_protocol.internal.serialize(
                        [self.client_id, QUERY_NUMBER, origin, dest] + list(intersection)
                    ))
        self.output_queue.send(message_protocol.internal.serialize([self.client_id]))

    def run(self):
        logging.info(f"[QUERY {QUERY_NUMBER}] Starting sg_detect worker")
        self.origins_queue.start_consuming(self._on_origins_message)
        self.destinations_queue.start_consuming(self._on_destinations_message)
        if not self.closed:
            self._emit_results()

    def close(self):
        try:
            self.closed = True
            self.origins_queue.stop_consuming()
            self.destinations_queue.stop_consuming()
            self.origins_queue.close()
            self.destinations_queue.close()
            self.output_queue.close()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error closing resources: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = SgDetect()
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[QUERY {QUERY_NUMBER}] Error in sg_detect: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
