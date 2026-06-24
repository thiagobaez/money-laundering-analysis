import logging
import os
import threading
import time

from common.message_protocol import internal
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ

HEARTBEAT_EXCHANGE = "heartbeat_exchange"
WATCHDOG_COUNT = int(os.environ.get("WATCHDOG_COUNT", "0"))
CONTAINER_NAME = os.environ.get("CONTAINER_NAME", "")
MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "3"))

class HeartbeatSender:
    def __init__(self):
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    def _run(self):
        
        if WATCHDOG_COUNT == 0:
            return
        routing_keys = [f"watchdog_{i}" for i in range(WATCHDOG_COUNT)]
        while not self._stop_event.is_set():
            sender = None
            try:
                sender = MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, HEARTBEAT_EXCHANGE, routing_keys
                )
                while not self._stop_event.is_set():
                    sender.send(internal.serialize({"container": CONTAINER_NAME, "ts": time.time()}))
                    time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                time.sleep(5)
            finally:
                if sender is not None:
                    try:
                        sender.close()
                    except Exception:
                        pass

def start_if_configured():
    if CONTAINER_NAME:
        sender = HeartbeatSender()
        sender.start()
        return sender
    return None
