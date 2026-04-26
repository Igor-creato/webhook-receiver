"""
Admin Panel.
Accessible only on 127.0.0.1:8098 (via SSH tunnel).
Provides UI for:
  - Database connection settings
  - Network management (add/edit/delete)
  - Field mapping editor (like Admitad screenshot)
  - Webhook URL generation
  - Recent webhook log viewer
  - Queue stats
"""
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Any

import redis
from fastapi import FastAPI, Form, Request, Response, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.config import (
    load,
    save,
    generate_secret_path,
    get_db_config,
    get_all_networks,
    DEFAULT_MAPPING,
    DEFAULT_STATUS_MAP,
)
from app.db import test_connection, get_affiliate_networks, get_recent_webhooks, get_distinct_order_statuses

ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")
if not ADMIN_SECRET or ADMIN_SECRET in ("changeme_on_first_run", "123"):
    raise SystemExit("ADMIN_SECRET env var must be set to a strong value (run install.sh to generate)")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
SESSION_COOKIE = "whk_session"
SESSION_TTL = 3600 * 8  # 8 hours

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))


def _make_session_token() -> str:
    return secrets.token_hex(32)


def _hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


_sessions: dict[str, float] = {}


def _check_auth(session: str | None) -> bool:
    if not session:
        return False
    expires = _sessions.get(session, 0)
    if expires < time.time():
        _sessions.pop(session, None)
        return False
    return True


def _get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)


def _get_queue_stats() -> dict[str, Any]:
    try:
        r = _get_redis()
        queue_len = r.llen("webhook:queue")
        dlq_len = r.llen("webhook:dlq")
        return {"queue": queue_len, "dlq": dlq_len}
    except Exception:
        return {"queue": "?", "dlq": "?"}


# --- Auth routes ---


@app.get("/", response_class=HTMLResponse)
async def root(request: Request, whk_session: str | None = Cookie(None)):
    if _check_auth(whk_session):
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse(request, "login.html", {"error": ""})


@app.post("/login")
async def login(request: Request, password: str = Form(...)):
    if hmac.compare_digest(password, ADMIN_SECRET):
        token = _make_session_token()
        _sessions[token] = time.time() + SESSION_TTL
        resp = RedirectResponse("/dashboard", status_code=302)
        resp.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="strict", max_age=SESSION_TTL)
        return resp
    return templates.TemplateResponse(request, "login.html", {"error": "Неверный пароль"})


@app.get("/logout")
async def logout(whk_session: str | None = Cookie(None)):
    if whk_session:
        _sessions.pop(whk_session, None)
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie(SESSION_COOKIE)
    return resp


# --- Auth middleware check helper ---
def _require_auth(session: str | None):
    if not _check_auth(session):
        raise _RedirectToLogin()


class _RedirectToLogin(Exception):
    pass


@app.exception_handler(_RedirectToLogin)
async def redirect_to_login(request, exc):
    return RedirectResponse("/", status_code=302)


# --- Dashboard ---


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    db_ok, db_msg = False, "Не настроено"
    if cfg["db"].get("host"):
        db_ok, db_msg = test_connection()

    networks = get_all_networks()
    stats = _get_queue_stats()

    return templates.TemplateResponse(request, "dashboard.html", {
        "db_ok": db_ok,
        "db_msg": db_msg,
        "db": cfg["db"],
        "networks": networks,
        "stats": stats,
        "network_count": len(networks),
    })


# --- DB Settings ---


@app.get("/db-settings", response_class=HTMLResponse)
async def db_settings_page(request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    db_ok, db_msg = False, ""
    if cfg["db"].get("host"):
        db_ok, db_msg = test_connection()
    return templates.TemplateResponse(request, "db_settings.html", {
        "db": cfg["db"], "db_ok": db_ok, "db_msg": db_msg,
    })


@app.post("/db-settings")
async def db_settings_save(
    request: Request,
    whk_session: str | None = Cookie(None),
    host: str = Form(""),
    port: int = Form(3306),
    user: str = Form(""),
    password: str = Form(""),
    database: str = Form(""),
    table_prefix: str = Form("wp_"),
):
    _require_auth(whk_session)

    # Sanitize prefix
    import re
    if not re.match(r'^[a-zA-Z0-9_]+$', table_prefix):
        table_prefix = "wp_"

    cfg = load()
    # Keep existing password when the form field is left empty (it is never
    # rendered back to the client to avoid leaking it via view-source).
    new_password = password if password else cfg["db"].get("password", "")
    cfg["db"] = {
        "host": host.strip(),
        "port": port,
        "user": user.strip(),
        "password": new_password,
        "database": database.strip(),
        "table_prefix": table_prefix.strip(),
    }
    save(cfg)

    return RedirectResponse("/db-settings", status_code=302)


@app.post("/db-test")
async def db_test(request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    ok, msg = test_connection()
    return {"ok": ok, "message": msg}


# --- Network Management ---


@app.get("/networks", response_class=HTMLResponse)
async def networks_page(request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    networks = get_all_networks()

    # Try to load from DB
    db_networks = []
    try:
        db_networks = get_affiliate_networks()
    except Exception:
        pass

    return templates.TemplateResponse(request, "networks.html", {
        "networks": networks,
        "db_networks": db_networks,
    })


@app.post("/networks/add")
async def network_add(
    request: Request,
    whk_session: str | None = Cookie(None),
    name: str = Form(""),
    slug: str = Form(""),
):
    _require_auth(whk_session)

    import re
    slug = re.sub(r'[^a-z0-9_-]', '', slug.lower().strip())
    name = name.strip()
    if not slug or not name:
        return RedirectResponse("/networks", status_code=302)

    cfg = load()
    if slug not in cfg["networks"]:
        cfg["networks"][slug] = {
            "name": name,
            "slug": slug,
            "secret_path": generate_secret_path(),
            "is_active": True,
            "rate_limit": 200,
            "mapping": dict(DEFAULT_MAPPING),
            "status_mapping": dict(DEFAULT_STATUS_MAP),
            "field_transforms": {},
        }
        save(cfg)

    return RedirectResponse(f"/networks/{slug}", status_code=302)


@app.post("/networks/import-from-db")
async def network_import(
    request: Request,
    whk_session: str | None = Cookie(None),
    network_id: int = Form(0),
    network_name: str = Form(""),
    network_slug: str = Form(""),
):
    _require_auth(whk_session)
    import re
    slug = re.sub(r'[^a-z0-9_-]', '', network_slug.lower().strip())
    name = network_name.strip()
    if not slug or not name:
        return RedirectResponse("/networks", status_code=302)

    cfg = load()
    if slug not in cfg["networks"]:
        cfg["networks"][slug] = {
            "name": name,
            "slug": slug,
            "db_network_id": network_id,
            "secret_path": generate_secret_path(),
            "is_active": True,
            "rate_limit": 200,
            "mapping": dict(DEFAULT_MAPPING),
            "status_mapping": dict(DEFAULT_STATUS_MAP),
            "field_transforms": {},
        }
        save(cfg)

    return RedirectResponse(f"/networks/{slug}", status_code=302)


@app.get("/networks/{slug}", response_class=HTMLResponse)
async def network_edit_page(slug: str, request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    network = cfg["networks"].get(slug)
    if not network:
        return RedirectResponse("/networks", status_code=302)

    # Build webhook URL using configured domain
    webhook_domain = os.environ.get("WEBHOOK_DOMAIN", "")
    if webhook_domain:
        webhook_url = f"https://{webhook_domain}/{slug}/{network.get('secret_path', '')}"
    else:
        webhook_url = f"http://localhost:8099/{slug}/{network.get('secret_path', '')}"

    order_statuses = get_distinct_order_statuses()

    return templates.TemplateResponse(request, "network_edit.html", {
        "network": network,
        "slug": slug,
        "webhook_url": webhook_url,
        "default_mapping": DEFAULT_MAPPING,
        "default_status_map": DEFAULT_STATUS_MAP,
        "order_statuses": order_statuses,
    })


@app.post("/networks/{slug}/save")
async def network_save(slug: str, request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    if slug not in cfg["networks"]:
        return RedirectResponse("/networks", status_code=302)

    form = await request.form()

    # Basic fields
    cfg["networks"][slug]["name"] = form.get("name", slug)
    cfg["networks"][slug]["is_active"] = form.get("is_active") == "on"
    webhook_method = form.get("webhook_method", "")
    if webhook_method in ("GET", "POST", "GET&POST"):
        cfg["networks"][slug]["webhook_method"] = webhook_method
    cfg["networks"][slug]["webhook_base_url"] = form.get("webhook_base_url", "")

    try:
        cfg["networks"][slug]["rate_limit"] = max(0, int(form.get("rate_limit", 200)))
    except (ValueError, TypeError):
        cfg["networks"][slug]["rate_limit"] = 200

    # Field mapping
    mapping = {}
    i = 0
    while True:
        field_key = form.get(f"map_field_{i}")
        field_val = form.get(f"map_param_{i}")
        if field_key is None:
            break
        if field_key.strip() and field_val.strip():
            mapping[field_key.strip()] = field_val.strip()
        i += 1

    if mapping:
        cfg["networks"][slug]["mapping"] = mapping

    # Status mapping
    status_map = {}
    j = 0
    while True:
        s_from = form.get(f"status_from_{j}")
        s_to = form.get(f"status_to_{j}")
        if s_from is None:
            break
        if s_from.strip() and s_to.strip():
            status_map[s_from.strip()] = s_to.strip()
        j += 1

    if status_map:
        cfg["networks"][slug]["status_mapping"] = status_map

    # Field transforms
    field_transforms = {}
    k = 0
    while True:
        t_field = form.get(f"transform_field_{k}")
        t_type = form.get(f"transform_type_{k}")
        if t_field is None:
            break
        if t_field.strip() and t_type.strip():
            field_transforms[t_field.strip()] = t_type.strip()
        k += 1
    cfg["networks"][slug]["field_transforms"] = field_transforms

    save(cfg)
    return RedirectResponse(f"/networks/{slug}", status_code=302)


@app.post("/networks/{slug}/regenerate-path")
async def network_regenerate_path(slug: str, request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    if slug in cfg["networks"]:
        cfg["networks"][slug]["secret_path"] = generate_secret_path()
        save(cfg)
    return RedirectResponse(f"/networks/{slug}", status_code=302)


@app.post("/networks/{slug}/toggle")
async def network_toggle(slug: str, request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    if slug in cfg["networks"]:
        cfg["networks"][slug]["is_active"] = not cfg["networks"][slug].get("is_active", False)
        save(cfg)
    return RedirectResponse("/networks", status_code=302)


@app.post("/networks/{slug}/delete")
async def network_delete(slug: str, request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    cfg = load()
    cfg["networks"].pop(slug, None)
    save(cfg)
    return RedirectResponse("/networks", status_code=302)


# --- Logs ---


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request, whk_session: str | None = Cookie(None)):
    _require_auth(whk_session)
    webhooks = get_recent_webhooks(100)
    stats = _get_queue_stats()
    return templates.TemplateResponse(request, "logs.html", {
        "webhooks": webhooks, "stats": stats,
    })


@app.get("/health")
async def health():
    return PlainTextResponse("ok")
