import docker
import random
import time
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CHAOS] %(message)s",
)


def main():
    interval = float(os.environ.get("KILL_INTERVAL", "30"))
    exclude_raw = os.environ.get("EXCLUDE_CONTAINERS", "rabbitmq,chaos_monkey")
    exclude = set(name.strip() for name in exclude_raw.split(",") if name.strip())

    client = docker.from_env()

    logging.info(f"Started. Kill interval: {interval}s | Excluded: {exclude}")

    while True:
        time.sleep(interval)

        try:
            containers = client.containers.list()
        except Exception as e:
            logging.error(f"Could not list containers: {e}")
            continue

        candidates = [c for c in containers if c.name not in exclude]

        if not candidates:
            logging.info("No candidates to kill, skipping.")
            continue

        target = random.choice(candidates)
        logging.info(f"Killing: {target.name}")
        try:
            target.kill()
            logging.info(f"Killed: {target.name}")
        except Exception as e:
            logging.error(f"Failed to kill {target.name}: {e}")


if __name__ == "__main__":
    main()
