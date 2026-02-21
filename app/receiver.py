"""
Webhook Receiver.
Accepts GET/POST on /{slug}/{secret} and /wh/{slug}/{secret}
Immediately pushes raw data to Redis queue and returns 200.
"""
import json
import logging
import os
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

from app.config import get_network

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("webhook.receiver")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "webhook:queue"
RATE_LIMIT_PREFIX = "webhook:rl:"

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


@app.get("/health")
async def health():
    return PlainTextResponse("ok")


def _is_safe_slug(s: str) -> bool:
    if not s or len(s) > 64:
        return False
    return all(c.isalnum() or c in "-_" for c in s)


async def _handle_webhook(slug: str, secret: str, request: Request):
    """Main webhook handler."""
    if not _is_safe_slug(slug):
        return PlainTextResponse("bad request", status_code=400)

    network = get_network(slug)
    if network is None:
        return PlainTextResponse("not found", status_code=404)

    if not network.get("is_active", False):
        return PlainTextResponse("disabled", status_code=403)

    if network.get("secret_path", "") != secret:
        return PlainTextResponse("forbidden", status_code=403)

    # Check allowed HTTP method
    webhook_method = network.get("webhook_method", "")
    if webhook_method and webhook_method != "GET&POST":
        if request.method != webhook_method:
            return PlainTextResponse("method not allowed", status_code=405)

    # Rate limiting
    rate_limit = int(network.get("rate_limit", 200))
    r = await get_redis()
    if rate_limit > 0:
        rl_key = f"{RATE_LIMIT_PREFIX}{slug}"
        current = await r.incr(rl_key)
        if current == 1:
            await r.expire(rl_key, 60)
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

    await r.lpush(QUEUE_KEY, message)

    stats_key = f"webhook:stats:{slug}:{int(time.time()) // 3600}"
    await r.incr(stats_key)
    await r.expire(stats_key, 86400 * 7)

    return PlainTextResponse("ok")


# Both URL patterns
app.add_api_route("/{slug}/{secret}", _handle_webhook, methods=["GET", "POST"], response_class=PlainTextResponse)