"""
Email sender for webhook worker.
Sends transaction notifications directly via SMTP,
independent of WordPress / WP Cron.

Sender name/email are read from WordPress settings (wp_options):
  - cashback_email_sender_name  (fallback: blogname)
  - cashback_email_sender_email (fallback: admin_email)

Site URL is built from DOMAIN env var (shared .env).
"""
import logging
import os
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any

from app.db import get_conn

logger = logging.getLogger("webhook.email")

# SMTP configuration from environment (shared .env)
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
# SMTP_SECURE=ssl → port 465 implicit SSL; SMTP_SECURE=tls → port 587 STARTTLS
SMTP_SECURE = os.environ.get("SMTP_SECURE", "ssl").lower()

# Domain from shared .env (e.g. "site.automatization-bot.ru")
DOMAIN = os.environ.get("DOMAIN", "")


def is_configured() -> bool:
    """Check if SMTP is configured."""
    return bool(SMTP_HOST)


def _get_site_url() -> str:
    """Build site URL from DOMAIN env var."""
    if DOMAIN:
        return f"https://{DOMAIN.strip('/')}"
    return ""


# =====================================================================
# WordPress settings cache (from wp_options)
# =====================================================================

_wp_settings_cache: dict[str, str] = {}
_wp_settings_ts: float = 0.0
_WP_CACHE_TTL = 300  # 5 minutes


def _get_wp_option(option_name: str) -> str | None:
    """Read a single option from wp_options."""
    from app.db import _prefix
    prefix = _prefix()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT `option_value` FROM `{prefix}options` "
                    f"WHERE `option_name` = %s LIMIT 1",
                    (option_name,),
                )
                row = cur.fetchone()
                if row is not None:
                    return row.get("option_value", "")
                return None
    except Exception:
        logger.exception("Failed to read wp_option %s", option_name)
        return None


def _get_sender_settings() -> tuple[str, str]:
    """
    Get sender name and email from WordPress settings.
    Priority matches Cashback_Email_Sender in PHP:
      name:  cashback_email_sender_name → blogname → 'Cashback'
      email: cashback_email_sender_email → admin_email → SMTP_USER
    Cached for 5 minutes.
    """
    import time
    global _wp_settings_cache, _wp_settings_ts

    now = time.time()
    if _wp_settings_cache and (now - _wp_settings_ts) < _WP_CACHE_TTL:
        return _wp_settings_cache.get("from_name", "Cashback"), _wp_settings_cache.get("from_email", "")

    # Read from DB
    sender_name = _get_wp_option("cashback_email_sender_name") or ""
    if not sender_name:
        sender_name = _get_wp_option("blogname") or "Cashback"

    sender_email = _get_wp_option("cashback_email_sender_email") or ""
    if not sender_email:
        sender_email = _get_wp_option("admin_email") or ""
    if not sender_email:
        sender_email = os.environ.get("SMTP_FROM") or os.environ.get("SMTP_FROM_EMAIL") or SMTP_USER or ""

    _wp_settings_cache = {"from_name": sender_name, "from_email": sender_email}
    _wp_settings_ts = now

    return sender_name, sender_email


# =====================================================================
# User data
# =====================================================================

def _get_user_email(user_id: int) -> tuple[str, str] | None:
    """
    Get user email and display_name from wp_users.
    Returns (email, display_name) or None.
    """
    from app.db import _prefix
    prefix = _prefix()
    table = f"{prefix}users"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT `user_email`, `display_name` FROM `{table}` "
                    f"WHERE `ID` = %s LIMIT 1",
                    (user_id,),
                )
                row = cur.fetchone()
                if row and row.get("user_email"):
                    return row["user_email"], row.get("display_name") or ""
                return None
    except Exception:
        logger.exception("Failed to get user email for user_id=%s", user_id)
        return None


def _is_notification_enabled(user_id: int, notification_type: str) -> bool:
    """
    Check if notification is enabled for this user.
    Checks both global setting (wp_options) and user preference.
    Returns True if enabled (default when no preference exists).
    """
    from app.db import _prefix
    prefix = _prefix()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check global setting: wp_options → cashback_notify_{type}
                cur.execute(
                    f"SELECT `option_value` FROM `{prefix}options` "
                    f"WHERE `option_name` = %s LIMIT 1",
                    (f"cashback_notify_{notification_type}",),
                )
                row = cur.fetchone()
                if row is not None:
                    val = row.get("option_value", "")
                    if val == "0":
                        return False

                # Check user preference
                cur.execute(
                    f"SELECT `enabled` FROM `{prefix}cashback_notification_preferences` "
                    f"WHERE `user_id` = %s AND `notification_type` = %s LIMIT 1",
                    (user_id, notification_type),
                )
                row = cur.fetchone()
                if row is not None:
                    return bool(int(row.get("enabled", 1)))

                return True
    except Exception:
        logger.exception("Failed to check notification preference for user_id=%s", user_id)
        return True


# =====================================================================
# HTML template
# =====================================================================

def _render_html(subject: str, body_text: str, site_name: str, user_id: int | None = None) -> str:
    """Render HTML email template (matches WordPress Cashback_Email_Sender style)."""
    site_url = _get_site_url() or "#"

    settings_link = ""
    if user_id and _get_site_url():
        settings_link = f"{_get_site_url()}/my-account/cashback-notifications/"

    html = (
        '<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
        f'<title>{_esc(subject)}</title></head>'
        '<body style="margin:0;padding:0;background:#f4f4f7;font-family:Arial,Helvetica,sans-serif;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f7;">'
        '<tr><td align="center" style="padding:24px 16px;">'
        '<table role="presentation" width="600" cellpadding="0" cellspacing="0" '
        'style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;width:100%;">'
        # Header
        f'<tr><td style="background:#2271b1;padding:20px 32px;">'
        f'<a href="{_esc(site_url)}" style="color:#ffffff;text-decoration:none;font-size:20px;font-weight:bold;">'
        f'{_esc(site_name)}</a></td></tr>'
        # Body
        f'<tr><td style="padding:32px;color:#333333;font-size:15px;line-height:1.6;">'
        f'<p style="white-space:pre-line;margin:0 0 16px;">{_esc(body_text)}</p>'
        '</td></tr>'
        # Footer
        '<tr><td style="padding:16px 32px;border-top:1px solid #eee;color:#999999;font-size:12px;">'
        '<p style="margin:0 0 8px;">Это автоматическое сообщение, не отвечайте на него.</p>'
    )
    if settings_link:
        html += (
            '<p style="margin:0;">'
            f'<a href="{_esc(settings_link)}" style="color:#2271b1;text-decoration:underline;">'
            'Настроить уведомления</a></p>'
        )
    html += '</td></tr></table></td></tr></table></body></html>'
    return html


def _esc(s: str) -> str:
    """Minimal HTML escaping."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# =====================================================================
# SMTP send
# =====================================================================

def _send_email(to: str, subject: str, html: str, from_name: str, from_email: str) -> bool:
    """Send email via SMTP. Supports SSL (port 465) and STARTTLS (port 587)."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = formataddr((str(Header(from_name, "utf-8")), from_email))
    msg["To"] = to
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        if SMTP_SECURE == "ssl":
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                if SMTP_USER and SMTP_PASSWORD:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(from_email, [to], msg.as_string())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                server.ehlo()
                if SMTP_SECURE == "tls":
                    server.starttls()
                    server.ehlo()
                if SMTP_USER and SMTP_PASSWORD:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(from_email, [to], msg.as_string())
        return True
    except Exception:
        logger.exception("SMTP send failed to=%s", to)
        return False


# =====================================================================
# Public API
# =====================================================================

def send_transaction_new(
    user_id: int,
    partner: str,
    offer_name: str,
    sum_order: Any,
    order_status: str,
) -> bool:
    """
    Send 'new transaction' email notification to user.
    Returns True if sent, False if skipped or failed.
    """
    if not is_configured():
        return False

    if user_id <= 0:
        return False

    if not _is_notification_enabled(user_id, "transaction_new"):
        logger.debug("Notification disabled for user_id=%s type=transaction_new", user_id)
        return False

    user_info = _get_user_email(user_id)
    if not user_info:
        return False

    email, display_name = user_info
    if not email:
        return False

    # Sender from WordPress settings
    from_name, from_email = _get_sender_settings()
    if not from_email:
        logger.warning("No sender email configured, skipping notification")
        return False

    shop = offer_name or "—"
    try:
        sum_formatted = f"{float(sum_order):,.2f}".replace(",", " ").replace(".", ",")
    except (ValueError, TypeError):
        sum_formatted = "—"

    site_url = _get_site_url()
    history_url = f"{site_url}/my-account/cashback-history/" if site_url else ""

    subject = f"Новая покупка в магазине {shop}"

    body = (
        f"Здравствуйте, {display_name or 'пользователь'}!\n\n"
        f"Ваша покупка зафиксирована.\n\n"
        f"Магазин: {shop}\n"
        f"Сумма заказа: {sum_formatted} ₽\n"
        f"Статус: В ожидании\n\n"
        f"Отслеживайте статус в личном кабинете: {history_url}"
    )

    html = _render_html(subject, body, from_name, user_id)
    sent = _send_email(email, subject, html, from_name, from_email)

    if sent:
        logger.info("Email sent to user_id=%s (%s): transaction_new", user_id, email)
    return sent
