"""
Database helper.
All queries use parameterized statements (%s placeholders via PyMySQL).
"""
import hashlib
import logging
import re
import time
from contextlib import contextmanager
from typing import Any, Generator

import pymysql
import pymysql.cursors

from app.config import get_db_config

logger = logging.getLogger("webhook.db")

_TABLE_PREFIX_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_prefix(prefix: str) -> str:
    if not _TABLE_PREFIX_RE.match(prefix):
        raise ValueError(f"Invalid table prefix: {prefix!r}")
    return prefix


def _prefix() -> str:
    db_cfg = get_db_config()
    return _validate_prefix(db_cfg.get("table_prefix", "wp_"))


@contextmanager
def get_conn() -> Generator[pymysql.connections.Connection, None, None]:
    db_cfg = get_db_config()
    if not db_cfg.get("host"):
        raise RuntimeError("Database not configured")
    conn = pymysql.connect(
        host=db_cfg["host"],
        port=int(db_cfg.get("port", 3306)),
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
        autocommit=False,
    )
    try:
        yield conn
    finally:
        conn.close()


def test_connection() -> tuple[bool, str]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
                if row and row.get("ok") == 1:
                    return True, "OK"
                return False, "Unexpected result"
    except Exception as e:
        return False, str(e)


def get_affiliate_networks() -> list[dict[str, Any]]:
    """Read networks from wp_cashback_affiliate_networks table."""
    prefix = _prefix()
    table = f"{prefix}cashback_affiliate_networks"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM `{table}` ORDER BY `name` ASC")
                return cur.fetchall()
    except Exception as e:
        logger.warning("Failed to read affiliate_networks: %s", e)
        return []


# ============================================================
# cashback_webhooks — raw payload storage
# Columns: id, payload, payload_norm, received_at
# ============================================================

def save_raw_webhook(payload_json: str, network_slug: str) -> int | None:
    """
    Insert into cashback_webhooks with SHA-256 deduplication.
    Returns row id or None if duplicate.
    """
    prefix = _prefix()
    table = f"{prefix}cashback_webhooks"
    payload_norm = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT IGNORE INTO `{table}` "
                    f"(`payload`, `payload_norm`, `received_at`) "
                    f"VALUES (%s, %s, NOW())",
                    (payload_json, payload_norm),
                )
                conn.commit()
                if cur.rowcount == 0:
                    return None  # duplicate
                return cur.lastrowid
    except Exception:
        logger.exception("Failed to save raw webhook")
        return None


def get_recent_webhooks(limit: int = 50) -> list[dict[str, Any]]:
    prefix = _prefix()
    table = f"{prefix}cashback_webhooks"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT `id`, `received_at`, "
                    f"LEFT(`payload`, 200) as payload_preview "
                    f"FROM `{table}` ORDER BY `id` DESC LIMIT %s",
                    (limit,),
                )
                return cur.fetchall()
    except Exception as e:
        logger.warning("Failed to get recent webhooks: %s", e)
        return []


# ============================================================
# wp_users — check user exists
# ============================================================

def check_user_exists(user_id: int) -> bool:
    prefix = _prefix()
    table = f"{prefix}users"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM `{table}` WHERE `ID` = %s LIMIT 1",
                    (user_id,),
                )
                return cur.fetchone() is not None
    except Exception:
        logger.exception("Failed to check user %s", user_id)
        return False


# ============================================================
# cashback_transactions / cashback_unregistered_transactions
# Matches actual schema with all columns
# ============================================================

def insert_transaction(data: dict[str, Any], registered: bool) -> tuple[bool, str]:
    """
    Insert into cashback_transactions or cashback_unregistered_transactions.
    
    Expected keys in data (from mapping):
        user_id, uniq_id, partner_name, offer_id, offer_name,
        order_number, order_status, sum_order, comission, currency,
        reward_ready, action_date, click_time, click_id, website_id,
        action_type
    """
    prefix = _prefix()

    if registered:
        table = f"{prefix}cashback_transactions"
    else:
        table = f"{prefix}cashback_unregistered_transactions"

    # Build idempotency key from uniq_id + partner
    idemp_src = f"{data.get('uniq_id', '')}_{data.get('partner_name', '')}_{data.get('user_id', '')}"
    idempotency_key = hashlib.sha256(idemp_src.encode("utf-8")).hexdigest()

    # Parse numeric values safely
    def safe_decimal(val, default=0.0):
        try:
            return float(val) if val else default
        except (ValueError, TypeError):
            return default

    def safe_int(val, default=None):
        try:
            return int(val) if val else default
        except (ValueError, TypeError):
            return default

    def safe_str(val, default=""):
        return str(val).strip() if val else default

    # Parse reward_ready to 0/1
    rr = safe_str(data.get("reward_ready", "0"))
    reward_ready = 1 if rr in ("1", "true", "yes") else 0

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = (
                    f"INSERT INTO `{table}` ("
                    f"  `user_id`, `uniq_id`, `partner`, `offer_id`, `offer_name`,"
                    f"  `order_number`, `order_status`, `sum_order`, `comission`, `currency`,"
                    f"  `reward_ready`, `action_date`, `click_time`, `click_id`, `website_id`,"
                    f"  `action_type`, `idempotency_key`"
                    f") VALUES ("
                    f"  %s, %s, %s, %s, %s,"
                    f"  %s, %s, %s, %s, %s,"
                    f"  %s, %s, %s, %s, %s,"
                    f"  %s, %s"
                    f")"
                )

                # Parse action_date and click_time — Admitad sends "2026-02-21 14:32:07"
                action_date = safe_str(data.get("action_date")) or None
                click_time = safe_str(data.get("click_time")) or None

                params = (
                    safe_int(data.get("user_id"), 0),           # user_id
                    safe_str(data.get("uniq_id")),               # uniq_id
                    safe_str(data.get("partner_name")),          # partner
                    safe_int(data.get("offer_id")),              # offer_id
                    safe_str(data.get("offer_name")),            # offer_name
                    safe_str(data.get("order_number")),          # order_number
                    safe_str(data.get("order_status", "waiting")),  # order_status
                    safe_decimal(data.get("sum_order")),         # sum_order
                    safe_decimal(data.get("comission")),         # comission
                    safe_str(data.get("currency", "RUB")) or "RUB",  # currency
                    reward_ready,                                 # reward_ready
                    action_date,                                  # action_date
                    click_time,                                   # click_time
                    safe_str(data.get("click_id")),              # click_id
                    safe_int(data.get("website_id")),            # website_id
                    safe_str(data.get("action_type")),           # action_type
                    idempotency_key,                              # idempotency_key
                )

                cur.execute(sql, params)
                conn.commit()
                return True, "OK"
    except pymysql.err.IntegrityError as e:
        if e.args[0] == 1062:  # Duplicate entry
            return False, "duplicate"
        if e.args[0] == 1452:  # FK constraint (user doesn't exist)
            return False, "fk_user_not_found"
        return False, str(e)
    except Exception as e:
        logger.exception("Failed to insert transaction")
        return False, str(e)


def update_transaction_status(
    uniq_id: str, partner_name: str, new_status: str
) -> tuple[bool, str]:
    """Update order_status for existing transaction."""
    prefix = _prefix()

    allowed = {"waiting", "completed", "declined"}
    if new_status not in allowed:
        return False, f"Invalid status: {new_status}"

    for tbl_suffix in ("cashback_transactions", "cashback_unregistered_transactions"):
        table = f"{prefix}{tbl_suffix}"
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE `{table}` SET `order_status` = %s "
                        f"WHERE `uniq_id` = %s AND `partner` = %s "
                        f"AND `order_status` NOT IN ('balance')",
                        (new_status, uniq_id, partner_name),
                    )
                    conn.commit()
                    if cur.rowcount > 0:
                        return True, "updated"
        except Exception as e:
            logger.warning("Update failed on %s: %s", table, e)

    return False, "not_found"