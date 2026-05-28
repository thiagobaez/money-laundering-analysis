import os
import shutil
import logging
import signal
import threading

from common import middleware, message_protocol

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
ORIGIN_EXCHANGE_NAME = os.environ["ORIGIN_EXCHANGE_NAME"]
ORIGIN_ROUTING_KEY = os.environ["ORIGIN_ROUTING_KEY"]
DESTINATION_EXCHANGE_NAME = os.environ["DESTINATION_EXCHANGE_NAME"]
DESTINATION_ROUTING_KEY = os.environ["DESTINATION_ROUTING_KEY"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MIN_COMMON = int(os.environ["MIN_COMMON"])
NUM_OG_WORKERS = int(os.environ.get("NUM_OG_WORKERS", "1"))
NUM_DT_WORKERS = int(os.environ.get("NUM_DT_WORKERS", "1"))

DATA_DIR = "/data"


class SgDetect:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.origins_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ORIGIN_EXCHANGE_NAME, [ORIGIN_ROUTING_KEY]
        )
        self.destinations_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, DESTINATION_EXCHANGE_NAME, [DESTINATION_ROUTING_KEY]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_id = None
        self.origins_eofs = 0
        self.destinations_eofs = 0

    def _handle_sigterm(self, signum, frame):
        self.close()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _origins_dir(self, client_id):
        return os.path.join(DATA_DIR, str(client_id), "origins")

    def _destinations_dir(self, client_id):
        return os.path.join(DATA_DIR, str(client_id), "destinations")

    def _on_origins_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            self.client_id = client_id

            if len(fields) == 2:
                self.origins_eofs += 1
                if self.origins_eofs >= NUM_OG_WORKERS:
                    logging.info(
                        f"[QUERY {QUERY_NUMBER}] [SG_DETECT] All origin EOFs received for client {client_id}"
                    )
                    self.origins_queue.stop_consuming()
                ack()
                return

            origin_account = fields[2]
            destinations = fields[3:]

            origins_dir = self._origins_dir(client_id)
            os.makedirs(origins_dir, exist_ok=True)
            with open(os.path.join(origins_dir, f"{origin_account}.csv"), "w") as f:
                f.write("\n".join(destinations) + "\n")

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing origin: {e}")
            nack()

    def _on_destinations_message(self, message, ack, nack):
        if self.closed:
            ack()
            return
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            self.client_id = client_id

            if len(fields) == 2:
                self.destinations_eofs += 1
                if self.destinations_eofs >= NUM_DT_WORKERS:
                    logging.info(
                        f"[QUERY {QUERY_NUMBER}] [SG_DETECT] All destination EOFs received for client {client_id}"
                    )
                    self.destinations_queue.stop_consuming()
                ack()
                return

            dest_account = fields[2]
            origins_of_dest = fields[3:]

            dest_dir = self._destinations_dir(client_id)
            os.makedirs(dest_dir, exist_ok=True)
            with open(os.path.join(dest_dir, f"{dest_account}.csv"), "w") as f:
                f.write("\n".join(origins_of_dest) + "\n")

            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing destination: {e}")
            nack()

    def _emit_results(self, client_id):
        origins_dir = self._origins_dir(client_id)
        dest_dir = self._destinations_dir(client_id)

        if not os.path.exists(origins_dir) or not os.path.exists(dest_dir):
            return

        dest_data = {}
        for dest_filename in os.listdir(dest_dir):
            dest_account = dest_filename[:-4]
            with open(os.path.join(dest_dir, dest_filename)) as f:
                dest_data[dest_account] = set(
                    line.strip() for line in f if line.strip()
                )

        results = []
        for origin_filename in os.listdir(origins_dir):
            origin_account = origin_filename[:-4]
            with open(os.path.join(origins_dir, origin_filename)) as f:
                destinations_of_origin = set(line.strip() for line in f if line.strip())

            for dest_account, origins_of_dest in dest_data.items():
                common = destinations_of_origin & origins_of_dest
                if len(common) >= MIN_COMMON:
                    results.append(
                        [origin_account, dest_account] + sorted(list(common))
                    )

        if results:
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, QUERY_NUMBER, results])
            )

    def run(self):

        origins_thread = threading.Thread(
            target=self.origins_queue.start_consuming, args=(self._on_origins_message,)
        )
        destinations_thread = threading.Thread(
            target=self.destinations_queue.start_consuming,
            args=(self._on_destinations_message,),
        )

        origins_thread.start()
        destinations_thread.start()
        origins_thread.join()
        destinations_thread.join()

        if not self.closed and self.client_id:
            self._emit_results(self.client_id)
            client_dir = os.path.join(DATA_DIR, str(self.client_id))
            if os.path.exists(client_dir):
                shutil.rmtree(client_dir)

        self.output_queue.send(message_protocol.internal.serialize([self.client_id]))

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
