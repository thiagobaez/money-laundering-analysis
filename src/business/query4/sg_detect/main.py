import os
import shutil
import logging
import signal
import threading

from common import middleware, message_protocol, checkpoint

QUERY_NUMBER = int(os.environ["QUERY_NUMBER"])
MOM_HOST = os.environ["MOM_HOST"]
DATA_DIR = os.environ.get("DATA_DIR", "/data")
ORIGIN_EXCHANGE_NAME = os.environ["ORIGIN_EXCHANGE_NAME"]
ORIGIN_ROUTING_KEY = os.environ["ORIGIN_ROUTING_KEY"]
DESTINATION_EXCHANGE_NAME = os.environ["DESTINATION_EXCHANGE_NAME"]
DESTINATION_ROUTING_KEY = os.environ["DESTINATION_ROUTING_KEY"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MIN_COMMON = int(os.environ["MIN_COMMON"])
NUM_OG_WORKERS = int(os.environ.get("NUM_OG_WORKERS", "1"))
NUM_DT_WORKERS = int(os.environ.get("NUM_DT_WORKERS", "1"))


class SgDetect:
    def __init__(self, heartbeat=None):
        self.closed = False
        self._lock = threading.Lock()
        self._heartbeat = heartbeat
        self.origins_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ORIGIN_EXCHANGE_NAME, [ORIGIN_ROUTING_KEY]
        )
        self.destinations_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, DESTINATION_EXCHANGE_NAME, [DESTINATION_ROUTING_KEY]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.origins_eofs: dict[str, int] = {}
        self.destinations_eofs: dict[str, int] = {}
        self._last_origins_hash: str | None = None
        self._last_destinations_hash: str | None = None
        self._load_checkpoint()

    def _load_checkpoint(self):
        state = checkpoint.load(DATA_DIR)
        if state is None:
            return
        self._last_origins_hash = state.get("last_origins_hash")
        self._last_destinations_hash = state.get("last_destinations_hash")
        self.origins_eofs = state.get("origins_eofs", {})
        self.destinations_eofs = state.get("destinations_eofs", {})
        logging.info(f"[QUERY {QUERY_NUMBER}] [SG_DETECT] Resumed from checkpoint")

    def _save_checkpoint(self):
        with self._lock:
            checkpoint.save(
                DATA_DIR,
                {
                    "last_origins_hash": self._last_origins_hash,
                    "last_destinations_hash": self._last_destinations_hash,
                    "origins_eofs": self.origins_eofs,
                    "destinations_eofs": self.destinations_eofs,
                },
            )

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

            if len(fields) == 2:
                with self._lock:
                    self.origins_eofs[client_id] = (
                        self.origins_eofs.get(client_id, 0) + 1
                    )
                    count = self.origins_eofs[client_id]
                    logging.info(
                        f"[QUERY {QUERY_NUMBER}] [SG_DETECT] Origins EOF received for client {client_id} ({count}/{NUM_OG_WORKERS})"
                    )
                    if count >= NUM_OG_WORKERS:
                        self._check_and_emit(client_id)
                self._save_checkpoint()
                ack()
                return

            h = checkpoint.msg_hash(message)
            if h == self._last_origins_hash:
                ack()
                return

            origin_account = fields[2]
            destinations = fields[3:]

            origins_dir = self._origins_dir(client_id)
            os.makedirs(origins_dir, exist_ok=True)
            with open(os.path.join(origins_dir, f"{origin_account}.csv"), "w") as f:
                f.write("\n".join(destinations) + "\n")

            self._last_origins_hash = h
            self._save_checkpoint()
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

            if len(fields) == 2:
                with self._lock:
                    self.destinations_eofs[client_id] = (
                        self.destinations_eofs.get(client_id, 0) + 1
                    )
                    count = self.destinations_eofs[client_id]
                    logging.info(
                        f"[QUERY {QUERY_NUMBER}] [SG_DETECT] Destinations EOF received for client {client_id} ({count}/{NUM_DT_WORKERS})"
                    )
                    if count >= NUM_DT_WORKERS:
                        self._check_and_emit(client_id)
                self._save_checkpoint()
                ack()
                return

            h = checkpoint.msg_hash(message)
            if h == self._last_destinations_hash:
                ack()
                return

            dest_account = fields[2]
            origins_of_dest = fields[3:]

            dest_dir = self._destinations_dir(client_id)
            os.makedirs(dest_dir, exist_ok=True)
            with open(os.path.join(dest_dir, f"{dest_account}.csv"), "w") as f:
                f.write("\n".join(origins_of_dest) + "\n")

            self._last_destinations_hash = h
            self._save_checkpoint()
            ack()
        except Exception as e:
            logging.error(f"[QUERY {QUERY_NUMBER}] Error processing destination: {e}")
            nack()

    def _check_and_emit(self, client_id):
        """Called with self._lock held. Emits results when both origins and destinations EOFs are complete."""
        og_done = self.origins_eofs.get(client_id, 0) >= NUM_OG_WORKERS
        dt_done = self.destinations_eofs.get(client_id, 0) >= NUM_DT_WORKERS
        if not (og_done and dt_done):
            return
        logging.info(f"[QUERY {QUERY_NUMBER}] [SG_DETECT] Both sides complete for client {client_id}, emitting results")

        self._emit_results(client_id)

        client_dir = os.path.join(DATA_DIR, str(client_id))
        if os.path.exists(client_dir):
            shutil.rmtree(client_dir)

        del self.origins_eofs[client_id]
        del self.destinations_eofs[client_id]

        self.output_queue.send(message_protocol.internal.serialize([client_id]))

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

    def close(self):
        if self._heartbeat:
            self._heartbeat.stop()
            self._heartbeat = None
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
    from common.heartbeat import start_if_configured

    heartbeat = start_if_configured()
    worker = SgDetect(heartbeat)
    signal.signal(signal.SIGTERM, lambda s, f: worker.close())
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
