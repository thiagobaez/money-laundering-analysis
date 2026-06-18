import hashlib
import json
import logging
import os


def msg_hash(body: bytes) -> str:
    return hashlib.md5(body).hexdigest()


def save(data_dir: str, state: dict):
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, "checkpoint.json")
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, path)
    except Exception as e:
        logging.warning(f"[CHECKPOINT] Failed to save to {path}: {e}")


def load(data_dir: str) -> dict | None:
    path = os.path.join(data_dir, "checkpoint.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            state = json.load(f)
        logging.info(f"[CHECKPOINT] Loaded from {path}")
        return state
    except Exception as e:
        logging.warning(f"[CHECKPOINT] Failed to load from {path}: {e}")
        return None
