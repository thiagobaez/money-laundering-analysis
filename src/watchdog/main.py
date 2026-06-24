import logging
import os
import signal
import threading
import time

import docker

from common.message_protocol import internal
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ

MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_TIMEOUT = float(os.environ["HEARTBEAT_TIMEOUT"])
WATCHDOG_ID = int(os.environ["WATCHDOG_ID"])
WATCHDOG_COUNT = int(os.environ["WATCHDOG_COUNT"])
WATCHDOG_HEARTBEAT_INTERVAL = float(os.environ["WATCHDOG_HEARTBEAT_INTERVAL"])
WATCHDOG_TIMEOUT = float(os.environ["WATCHDOG_TIMEOUT"])
REVIVE_INTERVAL = float(os.environ["REVIVE_INTERVAL"])
WORKER_EXCHANGE = os.environ["WORKER_EXCHANGE"]
PEER_EXCHANGE = os.environ["PEER_EXCHANGE"]

class Watchdog:
    def __init__(self):
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._docker = docker.from_env()
        self._last_seen: dict[str, float] = {}
        self._peer_last_seen: dict[int, float] = {}

        self._worker_queue = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, WORKER_EXCHANGE, [f"watchdog_{WATCHDOG_ID}"]
        )

        self._peer_queue = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, PEER_EXCHANGE, [f"peer_{WATCHDOG_ID}"]
        )

        peer_routing_keys = [f"peer_{i}" for i in range(WATCHDOG_COUNT) if i != WATCHDOG_ID]
        self._sender = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, PEER_EXCHANGE, peer_routing_keys
        ) if peer_routing_keys else None

    def _on_worker_hb(self, body, ack, nack):
        try:
            data = internal.deserialize(body)
            with self._lock:
                self._last_seen[data["container"]] = time.time()
            ack()
        except Exception as e:
            logging.error(f"[WATCHDOG {WATCHDOG_ID}] Error processing worker heartbeat: {e}")
            nack()

    def _on_peer_hb(self, body, ack, nack):
        try:
            data = internal.deserialize(body)
            peer_id = data["watchdog_id"]
            with self._lock:
                self._peer_last_seen[peer_id] = time.time()
            ack()
        except Exception as e:
            logging.error(f"[WATCHDOG {WATCHDOG_ID}] Error processing peer heartbeat: {e}")
            nack()

    def _is_leader(self):
        now = time.time()
        with self._lock:
            for peer_id in range(WATCHDOG_ID):
                ts = self._peer_last_seen.get(peer_id)
                if ts is not None and now - ts <= WATCHDOG_TIMEOUT:
                    return False
        return True

    def _send_heartbeat_loop(self):
        while not self._stopped.is_set():
            if self._sender is not None:
                try:
                    self._sender.send(
                        internal.serialize(
                            {"watchdog_id": WATCHDOG_ID, "ts": time.time()}
                        )
                    )
                except Exception as e:
                    logging.warning(
                        f"[WATCHDOG {WATCHDOG_ID}] Error sending peer heartbeat: {e}"
                    )
            time.sleep(WATCHDOG_HEARTBEAT_INTERVAL)

    def _try_restart(self, name: str) -> bool:
        try:
            self._docker.containers.get(name).start()
            logging.info(f"[WATCHDOG {WATCHDOG_ID}] Restarted: {name}")
            return True
        except Exception as e:
            logging.error(f"[WATCHDOG {WATCHDOG_ID}] Could not restart {name}: {e}")
            return False

    def _revive_loop(self):
        while not self._stopped.is_set():
            time.sleep(REVIVE_INTERVAL)
            now = time.time()

            if WATCHDOG_COUNT > 1:
                monitored_id = (WATCHDOG_ID - 1 + WATCHDOG_COUNT) % WATCHDOG_COUNT
                with self._lock:
                    peer_ts = self._peer_last_seen.get(monitored_id)
                if peer_ts is not None and now - peer_ts > WATCHDOG_TIMEOUT:
                    peer_name = f"watchdog_{monitored_id}"
                    logging.info(
                        f"[WATCHDOG {WATCHDOG_ID}] Peer {peer_name} timed out "
                        f"({now - peer_ts:.0f}s), restarting..."
                    )
                    if self._try_restart(peer_name):
                        with self._lock:
                            self._peer_last_seen[monitored_id] = time.time()

            if not self._is_leader():
                logging.debug(f"[WATCHDOG {WATCHDOG_ID}] Not leader, skipping worker check")
                continue

            logging.debug(f"[WATCHDOG {WATCHDOG_ID}] Leader — checking workers")
            with self._lock:
                snapshot = dict(self._last_seen)

            for name, ts in snapshot.items():
                if now - ts <= HEARTBEAT_TIMEOUT:
                    continue
                logging.info(
                    f"[WATCHDOG {WATCHDOG_ID}] No heartbeat from {name} "
                    f"for {now - ts:.0f}s, restarting..."
                )
                if self._try_restart(name):
                    with self._lock:
                        self._last_seen[name] = time.time()

    def run(self):
        logging.info(
            f"[WATCHDOG {WATCHDOG_ID}] Started (count={WATCHDOG_COUNT}, "
            f"heartbeat_timeout={HEARTBEAT_TIMEOUT}s, watchdog_timeout={WATCHDOG_TIMEOUT}s)"
        )
        threading.Thread(target=self._send_heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._revive_loop, daemon=True).start()
        threading.Thread(
            target=self._peer_queue.start_consuming,
            args=(self._on_peer_hb,),
            daemon=True,
        ).start()
        self._worker_queue.start_consuming(self._on_worker_hb)

    def close(self):
        self._stopped.set()
        try:
            self._worker_queue.stop_consuming()
        except Exception:
            pass
        try:
            self._peer_queue.connection.add_callback_threadsafe(
                self._peer_queue.stop_consuming
            )
        except Exception:
            pass
        for q in [self._sender, self._peer_queue, self._worker_queue]:
            if q is not None:
                try:
                    q.close()
                except Exception:
                    pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO)
    worker = Watchdog()
    signal.signal(signal.SIGTERM, lambda *_: worker.close())
    try:
        worker.run()
    except Exception as e:
        logging.error(f"[WATCHDOG {WATCHDOG_ID}] Fatal error: {e}")
        return 1
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    main()
