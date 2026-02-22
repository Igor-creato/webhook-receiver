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
_COLUMN_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$")

# Ключ в data dict → имя колонки в БД (только для несовпадений)
_FIELD_TO_COLUMN: dict[str, str] = {
    "partner_name": "partner",
}

# Типы полей для приведения значений
_FIELD_TYPES: dict[str, str] = {
    "user_id": "int",
    "offer_id": "int",
    "website_id": "int",
    "sum_order": "decimal",
    "comission": "decimal",
}

# Дефолтные значения (если значение пустое/отсутствует)
_FIELD_DEFAULTS: dict[str, Any] = {
    "user_id": 0,
    "order_status": "waiting",
    "currency": "RUB",
}


def _coerce_value(value: Any, field: str) -> Any:
    """Привести значение к нужному типу на основе имени поля."""
    field_type = _FIELD_TYPES.get(field, "str")
    default = _FIELD_DEFAULTS.get(field)

    if field_type == "int":
        try:
            return int(value) if value else (default if default is not None else None)
        except (ValueError, TypeError):
            return default if default is not None else None

    if field_type == "decimal":
        try:
            return float(value) if value else (default if default is not None else 0.0)
        except (ValueError, TypeError):
            return default if default is not None else 0.0

    # str
    result = str(value).strip() if value else ""
    if not result and default is not None:
        return default
    return result or None


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
# Columns: id, payload, payload_norm, network_slug, received_at
# ============================================================

def save_raw_webhook(payload_json: str, network_slug: str) -> int | None:
    """
    Insert into cashback_webhooks with deduplication.
    payload_norm is a VIRTUAL GENERATED column (json_normalize) — DB computes it.
    Returns row id or None if duplicate.
    """
    prefix = _prefix()
    table = f"{prefix}cashback_webhooks"

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT IGNORE INTO `{table}` "
                    f"(`payload`, `network_slug`, `received_at`) "
                    f"VALUES (%s, %s, NOW())",
                    (payload_json, network_slug),
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
    Columns are built dynamically from data keys (driven by network mapping).
    """
    prefix = _prefix()

    if registered:
        table = f"{prefix}cashback_transactions"
    else:
        table = f"{prefix}cashback_unregistered_transactions"

    # Build idempotency key from uniq_id + partner
    idemp_src = f"{data.get('uniq_id', '')}_{data.get('partner_name', '')}_{data.get('user_id', '')}"
    idempotency_key = hashlib.sha256(idemp_src.encode("utf-8")).hexdigest()

    # Build columns and values dynamically from data
    columns: list[str] = []
    values: list[Any] = []

    for field, value in data.items():
        col_name = _FIELD_TO_COLUMN.get(field, field)
        if not _COLUMN_NAME_RE.match(col_name):
            logger.warning("Skipping invalid column name: %s", col_name)
            continue
        columns.append(f"`{col_name}`")
        values.append(_coerce_value(value, field))

    # Always add idempotency_key
    columns.append("`idempotency_key`")
    values.append(idempotency_key)

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(values))
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