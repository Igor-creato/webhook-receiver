"""
Webhook Receiver.
Accepts GET/POST on /{slug}/{secret}.
Immediately pushes raw data to Redis queue and returns 200.
"""
import json
import logging
import os
import re
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import get_network

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("webhook.receiver")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "webhook:queue"
RATE_LIMIT_PREFIX = "webhook:rl:"
MAX_PAYLOAD_BYTES = 512 * 1024  # 512 KB

# Lua script: atomic INCR + EXPIRE (only sets TTL on first request)
_RL_LUA = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], 60)
end
return current
"""

app = FastAPI(
    title="Webhook Receiver",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

_redis_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            max_connections=20,
        )
    return _redis_pool


@app.on_event("shutdown")
async def shutdown():
    global _redis_pool
    if _redis_pool:
        await _redis_pool.close()


def _not_found() -> PlainTextResponse:
    return PlainTextResponse("404 Not Found", status_code=404)


@app.exception_handler(StarletteHTTPException)
async def _http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return _not_found()
    return PlainTextResponse(exc.detail or "", status_code=exc.status_code)


@app.get("/health")
async def health():
    return PlainTextResponse("ok")


_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


def _is_safe_slug(s: str) -> bool:
    return bool(s and _SLUG_RE.match(s))


async def _handle_webhook(slug: str, secret: str, request: Request):
    """Main webhook handler."""
    if not _is_safe_slug(slug):
        return _not_found()

    network = get_network(slug)
    if network is None:
        return _not_found()

    if not network.get("is_active", False):
        return _not_found()

    if network.get("secret_path", "") != secret:
        return _not_found()

    # Check allowed HTTP method
    webhook_method = network.get("webhook_method", "")
    if webhook_method and webhook_method != "GET&POST":
        if request.method != webhook_method:
            return PlainTextResponse("method not allowed", status_code=405)

    # Reject oversized requests early (before body parsing)
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_BYTES:
        return PlainTextResponse("payload too large", status_code=413)

    # Rate limiting (atomic: INCR + EXPIRE in single Lua script)
    rate_limit = int(network.get("rate_limit", 200))
    r = await get_redis()
    if rate_limit > 0:
        rl_key = f"{RATE_LIMIT_PREFIX}{slug}"
        current = await r.eval(_RL_LUA, 1, rl_key)
        if current > rate_limit:
            logger.warning("Rate limit exceeded for network %s", slug)
            return PlainTextResponse("rate limited", status_code=429)

    # Extract parameters
    params: dict[str, Any] = {}

    for key, value in request.query_params.items():
        params[key] = value

    if request.method == "POST":
        content_type = request.headers.get("content-type", "")
        try:
            if "application/json" in content_type:
                body = await request.json()
                if isinstance(body, dict):
                    params.update(body)
            elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
                form = await request.form()
                for key, value in form.items():
                    params[key] = value
            else:
                body_bytes = await request.body()
                if body_bytes:
                    try:
                        body = json.loads(body_bytes)
                        if isinstance(body, dict):
                            params.update(body)
                    except (json.JSONDecodeError, ValueError):
                        pass
        except Exception:
            pass

    if not params:
        return PlainTextResponse("no data", status_code=400)

    message = json.dumps(
        {
            "slug": slug,
            "params": params,
            "received_at": time.time(),
            "ip": request.client.host if request.client else "unknown",
        },
        ensure_ascii=False,
        default=str,
    )

    if len(message.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        logger.warning("Assembled payload exceeds limit for %s", slug)
        return PlainTextResponse("payload too large", status_code=413)

    await r.lpush(QUEUE_KEY, message)

    stats_key = f"webhook:stats:{slug}:{int(time.time()) // 3600}"
    await r.incr(stats_key)
    await r.expire(stats_key, 86400 * 7)

    return PlainTextResponse("ok")


# Both URL patterns
app.add_api_route("/{slug}/{secret}", _handle_webhook, methods=["GET", "POST"], response_class=PlainTextResponse)