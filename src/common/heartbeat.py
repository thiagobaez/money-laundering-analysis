import logging
import os
import threading
import time

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.message_protocol import internal


class HeartbeatSender:
    def __init__(self, container_name: str, mom_host: str, interval: float = 10.0):
        self._container_name = container_name
        self._mom_host = mom_host
        self._interval = interval
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def _run(self):
        while True:
            try:
                queue = MessageMiddlewareQueueRabbitMQ(self._mom_host, "heartbeat_queue")
                while True:
                    queue.send(internal.serialize({
                        "container": self._container_name,
                        "ts": time.time(),
                    }))
                    time.sleep(self._interval)
            except Exception as e:
                logging.warning(f"[HEARTBEAT] {self._container_name}: {e}, retrying in 5s")
                time.sleep(5)


def start_if_configured():
    name = os.environ.get("CONTAINER_NAME", "")
    host = os.environ.get("MOM_HOST", "rabbitmq")
    interval = float(os.environ.get("HEARTBEAT_INTERVAL", "10"))
    if name:
        HeartbeatSender(name, host, interval).start()
