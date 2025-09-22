#!/usr/bin/env python3
"""
Single-file FastAPI app that exposes Redis-stored price items at:

    GET /api

By default it looks for keys with prefix "prices" (same as your scraper).
You can override via environment vars or request query params.

Examples:
    # default
    curl http://localhost:8000/api

    # specify prefix
    curl "http://localhost:8000/api?prefix=prices"

    # filter by source (mohammadrahimi or seketehran)
    curl "http://localhost:8000/api?source=mohammadrahimi"

    # filter by slug substring
    curl "http://localhost:8000/api?slug=gold"

    # include the original raw JSON in the response (default=false)
    curl "http://localhost:8000/api?include_raw=true"
"""
from __future__ import annotations
import os
import json
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from redis.asyncio import Redis

# --- Defaults (match your scraper defaults) ---
DEFAULT_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DEFAULT_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "prices")
DEFAULT_SCAN_COUNT = int(os.getenv("REDIS_SCAN_COUNT", "100"))  # batch size for scanning/mget

# --- Logging ---
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("fastapi_redis_api")

# --- FastAPI app ---
app = FastAPI(title="Redis Price API", version="1.0.0")

# allow wide-open CORS by default (adjust for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


# --- Startup / Shutdown: init Redis client and attach to app.state ---
@app.on_event("startup")
async def startup_event():
    redis_url = os.getenv("REDIS_URL", DEFAULT_REDIS_URL)
    logger.info("Starting app: connecting to Redis at %s", redis_url)
    app.state.redis = Redis.from_url(redis_url, decode_responses=True)
    # test connection
    try:
        await app.state.redis.ping()
    except Exception as exc:
        logger.exception("Unable to connect to Redis at startup: %s", exc)
        # allow app to start but warn the user


@app.on_event("shutdown")
async def shutdown_event():
    redis: Optional[Redis] = getattr(app.state, "redis", None)
    if redis:
        try:
            await redis.close()
            await redis.connection_pool.disconnect()
            logger.info("Redis connection closed.")
        except Exception:
            logger.exception("Error while closing Redis connection.")


# --- Utility helpers ---
async def scan_keys(redis: Redis, match: str, batch_size: int = DEFAULT_SCAN_COUNT):
    """
    Async generator that yields lists of keys in batches.
    Uses SCAN (through scan_iter) to avoid blocking Redis on many keys.
    """
    batch: List[str] = []
    async for key in redis.scan_iter(match=match, count=batch_size):
        batch.append(key)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def parse_prefixed_key(key: str, prefix: str) -> Dict[str, Optional[str]]:
    """
    Given a key like "<prefix>:<source>:<slug>" return parts.
    If it doesn't split into 3 parts, returns key in 'raw_key' and leaves others None.
    """
    parts = key.split(":", 2)
    if len(parts) == 3 and parts[0] == prefix:
        _, source, slug = parts
        return {"prefix": prefix, "source": source, "slug": slug}
    if len(parts) >= 2:
        # maybe different prefix, try to be helpful
        return {"prefix": parts[0], "source": parts[1], "slug": parts[2] if len(parts) > 2 else None}
    return {"prefix": None, "source": None, "slug": None}


# --- New: normalize updated_at to HH:MM:SS (Latin digits) ---
PERSIAN_DIGITS_TRANS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def normalize_updated_at(raw: Optional[Any]) -> Optional[str]:
    """
    Normalize various updated_at representations into 'HH:MM:SS' using Latin digits.
    - Converts Persian/Arabic digits to Latin.
    - Extracts time portion if value contains date+time (e.g. '۱۴۰۴/۶/۳۱ ۱۹:۰۸:۴۳').
    - Parses ISO datetime strings when possible.
    - Returns None if raw is falsy.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Translate Persian/Arabic digits to Latin digits
    s = s.translate(PERSIAN_DIGITS_TRANS)

    # Normalize some common unicode separators to ASCII colon if present
    s = s.replace("\uFF1A", ":")  # fullwidth colon → ':'

    # Try to find a time pattern like HH:MM:SS anywhere in the string
    m = re.search(r"(\d{1,2}:\d{1,2}:\d{1,2})", s)
    if m:
        t = m.group(1)
        parts = t.split(":")
        if len(parts) == 3:
            parts = [p.zfill(2) for p in parts]
            return ":".join(parts)

    # Try ISO datetime parse (e.g., '2025-09-22T19:22:21' or '2025-09-22 19:22:21')
    try:
        # datetime.fromisoformat accepts 'YYYY-MM-DD HH:MM:SS' and 'YYYY-MM-DDTHH:MM:SS'
        dt = datetime.fromisoformat(s)
        return dt.time().strftime("%H:%M:%S")
    except Exception:
        pass

    # As a last resort, try to extract numbers that look like HHMMSS or HH:MM
    m2 = re.search(r"(\d{2}:\d{2})", s)
    if m2:
        hhmm = m2.group(1)
        hh, mm = hhmm.split(":")
        return f"{hh.zfill(2)}:{mm.zfill(2)}:00"

    # If nothing matched, return the cleaned string (but caller may expect a time)
    return s


# --- Endpoints ---
@app.get("/health")
async def health():
    """Simple health check."""
    redis: Optional[Redis] = getattr(app.state, "redis", None)
    ok = False
    try:
        if redis:
            ok = await redis.ping()
    except Exception:
        ok = False
    return {"ok": bool(ok)}


@app.get("/api")
async def get_all_prices(
    prefix: str = Query(DEFAULT_KEY_PREFIX, description="Redis key prefix to look for (default: prices)"),
    source: Optional[str] = Query(None, description="Filter by source (e.g. mohammadrahimi, seketehran)"),
    slug: Optional[str] = Query(None, description="Filter by slug substring"),
    include_raw: bool = Query(False, description="Include raw JSON stored in Redis (default false)"),
    limit: Optional[int] = Query(None, description="Optional hard limit on number of items returned"),
):
    """
    Return all items stored in Redis for the given prefix.

    Response is a JSON list of objects:
    [
      {
        "key": "<full redis key>",
        "prefix": "prices",
        "source": "mohammadrahimi",
        "slug": "some_item",
        "buy_price": 12345,
        "sell_price": 12300,
        "updated_at": "19:22:21",
        "raw": {...}  # optional when include_raw=true
      },
      ...
    ]
    """
    redis: Optional[Redis] = getattr(app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis client not initialized")

    match = f"{prefix}:*"
    items: List[Dict[str, Any]] = []
    total = 0

    try:
        # iterate keys in batches and fetch values with MGET for efficiency
        async for key_batch in scan_keys(redis, match=match, batch_size=DEFAULT_SCAN_COUNT):
            if not key_batch:
                continue
            vals = await redis.mget(*key_batch)
            for key, val in zip(key_batch, vals):
                meta = parse_prefixed_key(key, prefix)
                # allow filtering by source or slug substring if requested
                if source and meta.get("source") != source:
                    continue
                if slug and (meta.get("slug") is None or slug not in meta.get("slug")):
                    continue

                parsed: Dict[str, Any] = {
                    "key": key,
                    "prefix": meta.get("prefix"),
                    "source": meta.get("source"),
                    "slug": meta.get("slug"),
                    "buy_price": None,
                    "sell_price": None,
                    "updated_at": None,
                }

                # parse JSON payload if possible (your scraper writes JSON)
                if val:
                    try:
                        payload = json.loads(val)
                    except Exception:
                        payload = None

                    if payload and isinstance(payload, dict):
                        parsed["buy_price"] = payload.get("buy_price")
                        parsed["sell_price"] = payload.get("sell_price")
                        # Normalize updated_at to HH:MM:SS (Latin digits)
                        parsed["updated_at"] = normalize_updated_at(payload.get("updated_at"))
                        if include_raw:
                            parsed["raw"] = payload
                    else:
                        # If value wasn't JSON, include raw if requested and attempt normalization
                        if include_raw:
                            parsed["raw"] = val
                        parsed["updated_at"] = normalize_updated_at(val)

                items.append(parsed)
                total += 1
                if limit and total >= limit:
                    break
            if limit and total >= limit:
                break

    except Exception as exc:
        logger.exception("Error while reading from Redis: %s", exc)
        raise HTTPException(status_code=500, detail="Error while reading from Redis")

    return JSONResponse(content={"count": len(items), "items": items})


# --- CLI runner ---
if __name__ == "__main__":
    # allow overriding host/port via env vars
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    # uvicorn.run can take the ASGI app directly
    uvicorn.run(app, host=host, port=port, log_level=os.getenv("LOG_LEVEL", "info"))
