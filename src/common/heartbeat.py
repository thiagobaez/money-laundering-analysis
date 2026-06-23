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
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    def _run(self):
        while not self._stop_event.is_set():
            queue = None
            try:
                queue = MessageMiddlewareQueueRabbitMQ(
                    self._mom_host, "heartbeat_queue"
                )
                while not self._stop_event.is_set():
                    queue.send(
                        internal.serialize(
                            {
                                "container": self._container_name,
                                "ts": time.time(),
                            }
                        )
                    )
                    logging.debug(
                        f"[HEARTBEAT] {self._container_name}: Sending heartbeat"
                    )
                    time.sleep(self._interval)
            except Exception as e:
                logging.warning(
                    f"[HEARTBEAT] {self._container_name}: {e}, retrying in 5s"
                )
                time.sleep(5)
            finally:
                if queue is not None:
                    try:
                        queue.close()
                    except Exception:
                        pass


def start_if_configured():
    name = os.environ.get("CONTAINER_NAME", "")
    host = os.environ.get("MOM_HOST", "rabbitmq")
    interval = float(os.environ.get("HEARTBEAT_INTERVAL", "3"))
    if name:
        sender = HeartbeatSender(name, host, interval)
        sender.start()
        return sender
    return None
