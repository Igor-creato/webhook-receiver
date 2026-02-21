"""
Configuration manager.
Stores DB credentials, network configs, and field mappings in a JSON file.
Thread-safe reads/writes with file locking.
"""
import json
import os
import secrets
import threading
from pathlib import Path
from typing import Any

_lock = threading.Lock()
_CONFIG_PATH = os.environ.get("CONFIG_PATH", "/data/config.json")

_DEFAULT: dict[str, Any] = {
    "db": {
        "host": "",
        "port": 3306,
        "user": "",
        "password": "",
        "database": "",
        "table_prefix": "wp_",
    },
    "networks": {},
}


def _ensure_dir() -> None:
    Path(_CONFIG_PATH).parent.mkdir(parents=True, exist_ok=True)


def load() -> dict[str, Any]:
    with _lock:
        if not os.path.exists(_CONFIG_PATH):
            return json.loads(json.dumps(_DEFAULT))
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    merged = json.loads(json.dumps(_DEFAULT))
    merged.update(data)
    return merged


def save(cfg: dict[str, Any]) -> None:
    _ensure_dir()
    with _lock:
        tmp = _CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        os.replace(tmp, _CONFIG_PATH)


def generate_secret_path() -> str:
    return secrets.token_urlsafe(24)


def get_network(slug: str) -> dict[str, Any] | None:
    cfg = load()
    return cfg["networks"].get(slug)


def get_db_config() -> dict[str, Any]:
    return load()["db"]


def get_all_networks() -> dict[str, Any]:
    return load().get("networks", {})


DEFAULT_MAPPING: dict[str, str] = {
    "click_id": "subid1",
    "user_id": "subid2",
    "uniq_id": "admitad_id",
    "order_number": "order_id",
    "offer_id": "offer_id",
    "offer_name": "offer_name",
    "order_status": "payment_status",
    "sum_order": "order_sum",
    "comission": "payment_sum",
    "currency": "currency",
    "reward_ready": "reward_ready",
    "action_date": "time",
    "click_time": "click_time",
    "website_id": "website_id",
    "action_type": "type",
}

DEFAULT_STATUS_MAP: dict[str, str] = {
    "approved": "completed",
    "pending": "waiting",
    "declined": "declined",
    "rejected": "declined",
    "open": "waiting",
    "hold": "waiting",
}
