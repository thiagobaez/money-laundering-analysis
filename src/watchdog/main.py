import logging
import os
import signal
import threading
import time

import docker

from common.message_protocol import internal
from common import middleware

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
HEARTBEAT_TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "30"))


class Watchdog:
    def __init__(self):
        self._last_seen: dict[str, float] = {}
        self._lock = threading.Lock()
        self._docker = docker.from_env()
        self._input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, "heartbeat_queue"
        )

    def _on_message(self, body, ack, nack):
        try:
            data = internal.deserialize(body)
            with self._lock:
                self._last_seen[data["container"]] = time.time()
            ack()
        except Exception as e:
            logging.error(f"Error processing heartbeat: {e}")
            nack()

    def _revive_loop(self):
        while True:
            time.sleep(10)
            now = time.time()
            with self._lock:
                snapshot = dict(self._last_seen)

            for name, ts in snapshot.items():
                if now - ts <= HEARTBEAT_TIMEOUT:
                    continue
                logging.info(
                    f"No heartbeat from {name} for {now - ts:.0f}s, restarting..."
                )
                try:
                    self._docker.containers.get(name).start()
                    logging.info(f"Restarted: {name}")
                    with self._lock:
                        self._last_seen[name] = time.time()
                except Exception as e:
                    logging.error(f"Could not restart {name}: {e}")

    def run(self):
        logging.info(f"Started. Timeout={HEARTBEAT_TIMEOUT}s")
        revive_thread = threading.Thread(target=self._revive_loop, daemon=True)
        revive_thread.start()
        self._input_queue.start_consuming(self._on_message)

    def close(self):
        try:
            self._input_queue.stop_consuming()
            self._input_queue.close()
        except Exception as e:
            logging.error(f"Error closing watchdog: {e}")


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = Watchdog()
    signal.signal(signal.SIGTERM, lambda *_: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"Error in watchdog: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
