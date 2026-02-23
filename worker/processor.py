"""
Worker processor.
Consumes webhook messages from Redis queue (BRPOP),
applies field mapping, writes to MySQL.
Runs multiple threads for concurrency.
"""
import datetime
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any

import redis

from app.config import get_network, get_db_config, DEFAULT_STATUS_MAP
from app.db import (
    save_raw_webhook,
    check_user_exists,
    insert_transaction,
    update_transaction_status,
    update_transaction_fields,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("webhook.worker")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "webhook:queue"
DLQ_KEY = "webhook:dlq"  # dead letter queue
CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "4"))
SHUTDOWN = threading.Event()


def get_redis_conn() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)


def apply_mapping(params: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    """
    Transform incoming params using network mapping.
    mapping: {"our_field": "network_param_name"}
    Example: {"user_id": "subid2"} means params["subid2"] -> result["user_id"]
    """
    result: dict[str, Any] = {}
    for our_field, network_param in mapping.items():
        value = params.get(network_param, "")
        if value is None:
            value = ""
        result[our_field] = value
    return result


def resolve_status(raw_status: str, network_status_map: dict[str, str] | None) -> str:
    """Map network-specific status to our enum."""
    status_map = network_status_map or DEFAULT_STATUS_MAP
    raw_lower = str(raw_status).lower().strip()
    return status_map.get(raw_lower, "waiting")


def _convert_unix_timestamp(value: Any) -> str:
    """Convert Unix timestamp (seconds since epoch) to MySQL DATETIME string."""
    try:
        ts = float(value)
        # Valid range: 2000-01-01 to 2100-01-01
        if ts < 946684800 or ts > 4102444800:
            return str(value)
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(value)


def apply_field_transforms(
    data: dict[str, Any], transforms: dict[str, str]
) -> dict[str, Any]:
    """
    Apply value transformations to mapped fields.
    transforms: {"field_name": "transform_type"}
    """
    if not transforms:
        return data

    result = dict(data)
    for field, transform_type in transforms.items():
        value = result.get(field, "")
        if not value:
            continue
        if transform_type == "unix_timestamp":
            result[field] = _convert_unix_timestamp(value)
    return result


def process_message(raw_message: str) -> None:
    """Process a single webhook message."""
    try:
        msg = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in queue: %s", raw_message[:200])
        return

    slug = msg.get("slug", "")
    params = msg.get("params", {})
    received_at = msg.get("received_at", time.time())

    # Load network config
    network = get_network(slug)
    if network is None:
        logger.warning("Unknown network slug in queue: %s", slug)
        return

    mapping = network.get("mapping", {})
    status_mapping = network.get("status_mapping")

    # 1. Save raw webhook to cashback_webhooks
    payload_json = json.dumps(params, ensure_ascii=False, default=str)
    webhook_id = save_raw_webhook(payload_json, slug)
    if webhook_id is None:
        logger.info("Duplicate webhook for %s, skipping", slug)
        return

    # 2. Apply field mapping
    mapped = apply_mapping(params, mapping)

    # 2b. Apply field transforms (e.g. Unix timestamp -> datetime)
    field_transforms = network.get("field_transforms", {})
    mapped = apply_field_transforms(mapped, field_transforms)

    # 3. Resolve order status
    raw_status = mapped.get("order_status", "waiting")
    mapped["order_status"] = resolve_status(raw_status, status_mapping)

    # 4. Set partner_name from network config
    mapped["partner_name"] = network.get("name", slug)

    # 5. Validate required fields
    user_id_raw = mapped.get("user_id", "")
    uniq_id = mapped.get("uniq_id", "")

    if not uniq_id:
        logger.warning("No uniq_id in webhook for %s, webhook_id=%s", slug, webhook_id)
        return

    # 6. Try to parse user_id as int
    try:
        user_id = int(user_id_raw) if user_id_raw else 0
    except (ValueError, TypeError):
        user_id = 0

    mapped["user_id"] = user_id if user_id > 0 else user_id_raw

    # 7. Check if this is an update to existing transaction
    #    (same uniq_id + partner_name already exists)
    #    If order_status is completed/declined, try update first
    if mapped["order_status"] in ("completed", "declined"):
        ok, reason = update_transaction_fields(
            str(uniq_id), mapped["partner_name"],
            mapped.get("sum_order", ""), mapped.get("comission", ""),
            mapped["order_status"],
        )
        if ok:
            logger.info(
                "Updated transaction: %s/%s -> status=%s, sum=%s, com=%s",
                mapped["partner_name"], uniq_id, mapped["order_status"],
                mapped.get("sum_order"), mapped.get("comission"),
            )
            return

    # 8. Check if user is registered
    registered = False
    if user_id > 0:
        try:
            registered = check_user_exists(user_id)
        except Exception:
            registered = False

    # 9. Insert transaction
    ok, reason = insert_transaction(mapped, registered)
    if ok:
        target = "cashback_transactions" if registered else "cashback_unregistered_transactions"
        logger.info(
            "Inserted into %s: user=%s, uniq=%s, partner=%s, status=%s",
            target, user_id, uniq_id, mapped["partner_name"], mapped["order_status"],
        )
    elif reason == "duplicate":
        ok_upd, _ = update_transaction_fields(
            str(uniq_id), mapped["partner_name"],
            mapped.get("sum_order", ""), mapped.get("comission", ""),
            mapped["order_status"],
        )
        if ok_upd:
            logger.info(
                "Updated duplicate transaction fields: %s/%s -> status=%s, sum=%s, com=%s",
                mapped["partner_name"], uniq_id, mapped["order_status"],
                mapped.get("sum_order"), mapped.get("comission"),
            )
        else:
            logger.debug("Duplicate transaction, no changes: %s/%s", mapped["partner_name"], uniq_id)
    else:
        logger.error("Failed to insert: %s", reason)


def worker_loop(worker_id: int) -> None:
    """Single worker thread loop."""
    logger.info("Worker-%d started", worker_id)
    r = get_redis_conn()

    while not SHUTDOWN.is_set():
        try:
            # BRPOP blocks for 2 seconds max, then loops to check shutdown
            result = r.brpop(QUEUE_KEY, timeout=2)
            if result is None:
                continue

            _, raw_message = result
            try:
                process_message(raw_message)
            except Exception:
                logger.exception("Error processing message")
                # Push to dead letter queue
                try:
                    r.lpush(DLQ_KEY, raw_message)
                    r.ltrim(DLQ_KEY, 0, 9999)  # keep max 10k in DLQ
                except Exception:
                    pass

        except redis.ConnectionError:
            logger.error("Redis connection lost, reconnecting in 5s...")
            time.sleep(5)
            try:
                r = get_redis_conn()
            except Exception:
                pass
        except Exception:
            logger.exception("Unexpected error in worker loop")
            time.sleep(1)

    logger.info("Worker-%d stopped", worker_id)


def handle_signal(signum, frame):
    logger.info("Received signal %s, shutting down...", signum)
    SHUTDOWN.set()


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Wait for DB config
    logger.info("Starting worker with %d threads", CONCURRENCY)

    db_cfg = get_db_config()
    if not db_cfg.get("host"):
        logger.warning("Database not configured yet. Worker will retry when messages arrive.")

    threads = []
    for i in range(CONCURRENCY):
        t = threading.Thread(target=worker_loop, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    # Wait for shutdown
    while not SHUTDOWN.is_set():
        time.sleep(1)

    # Wait for threads
    for t in threads:
        t.join(timeout=10)

    logger.info("All workers stopped")


if __name__ == "__main__":
    main()
