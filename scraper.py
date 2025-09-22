#!/usr/bin/env python3
"""
Async worker that:
- fetches data from the mohammadrahimi API and a seketehran source
- seketehran can be scraped via HTTP (default) or a full browser with Playwright (for dynamic content)
- normalizes/parses prices and timestamps
- stores only buy_price, sell_price and updated_at for each item in Redis
- repeats every POLL_INTERVAL seconds (default 30)
- robust: retries, timeouts, logging, graceful shutdown

Usage:
    REDIS_URL=redis://localhost:6379/0 python scraper.py
    python scraper.py --use-playwright-for-sek
    python scraper.py --sek-url https://www.seketehran.ir/ --interval 15 --ttl 300
"""
from __future__ import annotations
import os
import asyncio
import signal
import json
import logging
import time
from typing import Any, Dict, Optional, Callable, List, Tuple
import argparse
from datetime import datetime, timezone
import re
from contextlib import AsyncExitStack

import httpx
from lxml import html
from redis.asyncio import Redis
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout, Playwright, Browser, Route, Request

# ---- Configuration defaults (can be overridden via env or CLI) ----
DEFAULT_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DEFAULT_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))  # seconds
DEFAULT_TTL = os.getenv("REDIS_TTL")  # seconds; if set, Redis keys expire automatically
DEFAULT_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "prices")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SEKETEHRAN_DEFAULT_URL = "https://www.seketehran.ir/"

# ---- Configure logging ----
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("price_updater")


# ---- Helper utilities ----
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_redis_key(prefix: str, source: str, slug: str) -> str:
    return f"{prefix}:{source}:{slug}"


def slugify_title(title: str) -> str:
    """
    Produce a predictable slug for Persian/Latin titles.
    - replace runs of whitespace / punctuation with single underscore
    - keep letters (including Persian), numbers, underscore and hyphen
    - trim repeated underscores and leading/trailing underscores
    """
    if not title:
        return ""
    s = title.strip()
    # Normalize common invisible chars
    s = s.replace("\u200c", "_").replace("\u200e", "").replace("\u200f", "")
    # Replace any sequence of characters that are NOT letters, numbers, underscore, or hyphen with underscore.
    # Include a generous range for Arabic/Persian letters.
    s = re.sub(r"[^\w\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF-]+", "_", s, flags=re.UNICODE)
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s.lower()


def parse_price(val: Any) -> Optional[int]:
    """
    Robustly parses a price value from various formats (int, str, None)
    and handles Persian/Arabic digits and thousands separators.
    Returns int or None for invalid/non-positive values.
    """
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            iv = int(val)
            return iv if iv >= 0 else None

        s = str(val).strip()
        if not s:
            return None

        # Normalize Persian/Arabic digits to Latin digits
        s = s.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789"))

        # Remove any non-digit characters except optional leading minus
        # (commas, currency symbols, non-breaking spaces)
        s = re.sub(r"[^\d\-]", "", s)
        if not s:
            return None

        iv = int(s)
        return iv if iv >= 0 else None
    except (ValueError, TypeError):
        return None


async def set_redis_json(redis: Redis, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
    payload = json.dumps(value, ensure_ascii=False)
    if ttl:
        await redis.set(key, payload, ex=int(ttl))
    else:
        await redis.set(key, payload)


# ---- Retry helper for network calls (honors Retry-After on 429) ----
async def with_retries(fn: Callable[..., Any], *args, retries: int = 3, backoff_base: float = 0.5, **kwargs):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            wait = backoff_base * (2 ** (attempt - 1))
            # If it's an HTTP 429 with Retry-After, honor it
            if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None and exc.response.status_code == 429:
                ra = exc.response.headers.get("Retry-After")
                if ra and ra.isdigit():
                    wait = float(ra)
                    logger.warning("Received 429; honoring Retry-After=%s seconds", ra)

            logger.warning("Attempt %d/%d failed: %s — retrying in %.2fs", attempt, retries, str(exc), wait)
            await asyncio.sleep(wait)
    logger.error("All %d attempts failed. Last exception: %s", retries, last_exc)
    raise last_exc


# ---- Scraper implementations ----

# 1) mohammadrahimi API fetch
MOH_RAM_ENDPOINT = "https://api.mohammadrahimi.com/products"
MOH_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,fa;q=0.8",
    "Cache-Control": "no-cache",
    "Origin": "https://panel.mohammadrahimi.com",
    "Referer": "https://panel.mohammadrahimi.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36",
    "x-verify": "254689384",
    # Cookie left as in original, but might be unnecessary; kept for compatibility
    "Cookie": "access_token_web=eyJpdiI6ImhWb3ZPMHl4NDJkbkFET3RjRUJ1bEE9PSIsInZhbHVlIjoidGt4NXYyU0NrK1VSNGppdERSdlBRcGdraWdiNjhnZEhZMmRHSEI1eEdKeEozZlNHR3Ywakd0Nk1RSjhTWGRVa0FUMVpHK1VRWmYwbHdkYm1vS0x0c21PWHNNVkxwaXBFbm5KMnNnaGQ1dUNMRDFSaW5DM2hBNnZIQXpWT3RKaTdrRExreTJPRElSN0Y3UGl6MkY3bUM4cEJVWk9vVzVuWm5xUzZYWUFiekJQNUk4QlFDQ1NPakhSaWhJTkh0Q1hTRnZIVjhPNEFRakxvVjlWQjdMYmgyakZxRURqak5YYWM3ZE5VZXA0VDlHbWE4V3VsVkdEejVpWWVaMUFBN2ZPR09aZVRJN1lub2Y2L2U0WFNmTnlCYkFyMTZiWEtqeWh0MkJWMTNGbFZ4N2drdjhicjNMUUcrS2FYdjQ1eEY2bVMiLCJtYWMiOiI4NTg5ZjQ0ZDYxYjVjM2YwOTJiZjIzNTBhMDE5ZmNhZThkMWNiMjBjNWJmM2FlN2VmZjk1YzhiMzg5YmM1ZTM3IiwidGFnIjoiIn0%3D"
}


async def fetch_mohammadrahimi(client: httpx.AsyncClient) -> Dict[str, Dict[str, Optional[Any]]]:
    """
    Fetch /products and return a dict mapping slug -> minimal payload dict.
    """
    logger.debug("Fetching mohammadrahimi API: %s", MOH_RAM_ENDPOINT)

    async def _do_fetch():
        resp = await client.get(MOH_RAM_ENDPOINT, headers=MOH_HEADERS, timeout=10.0)
        resp.raise_for_status()
        return resp.json()

    payload = await with_retries(_do_fetch, retries=3, backoff_base=0.5)
    out: Dict[str, Dict[str, Optional[Any]]] = {}
    data = payload.get("data") if isinstance(payload, dict) else None
    if not data:
        logger.warning("mohammadrahimi: no data found in API response")
        return out

    for category in data:
        products = category.get("products", []) or []
        for p in products:
            title = p.get("title", "<no title>")
            slug = slugify_title(title)
            updated_raw = p.get("updated_at") or p.get("updated_at_str") or p.get("updated_at_time")
            updated_at = str(updated_raw).strip() if updated_raw else None
            out[slug] = {
                "buy_price": parse_price(p.get("buy_price")),
                "sell_price": parse_price(p.get("sell_price")),
                "updated_at": updated_at,
            }
    return out


# 2) seketehran Parsers and Fetchers

# Allowed items (exact slugs we want to save)
ALLOWED_SEKETEHRAN_SLUGS = {
    "سکه_ربع_بهار",
    "سکه_یک_گرمی",
    "نیم_بهار_زیر_86",
    "ربع_بهار_زیر_86",
    "سکه_تصویر_امامی",
    "سکه_تمام_قدیم",
    "سکه_نیم_بهار",
    "امامی_زیر_86",
}


def parse_seketehran_html(html_text: str) -> Dict[str, Dict[str, Optional[Any]]]:
    """Parses HTML from seketehran.ir using lxml and returns only allowed items."""
    tree = html.fromstring(html_text)
    # Updated time text appears in a container like: <div class="table_updated__..."><span>...</span><span>date</span><span>time</span></div>
    updated_time_nodes = tree.xpath("//div[contains(@class,'table_updated')]/span/text()")
    updated_time = None
    if updated_time_nodes:
        # Last two spans often contain date and time; join them if present
        if len(updated_time_nodes) >= 2:
            # take last two pieces and join with space
            updated_time = " ".join([s.strip() for s in updated_time_nodes[-2:]])
        else:
            updated_time = updated_time_nodes[-1].strip()

    rows = tree.xpath("//div[contains(@class,'table_row')]")
    out: Dict[str, Dict[str, Optional[Any]]] = {}
    for r in rows:
        title_nodes = r.xpath(".//div[contains(@class,'table_itemTitle')]/text()")
        title = title_nodes[0].strip().replace("\u200e", "").replace("\u200f", "") if title_nodes else ""
        if not title:
            continue

        slug = slugify_title(title)
        # Only keep allowed items
        if slug not in ALLOWED_SEKETEHRAN_SLUGS:
            continue

        buy_nodes = r.xpath(".//div[contains(@class,'table_itemBuyPriceValue')]/text()")
        sell_nodes = r.xpath(".//div[contains(@class,'table_itemSellPriceValue')]/text()")
        buy_text = buy_nodes[0].strip() if buy_nodes else ""
        sell_text = sell_nodes[0].strip() if sell_nodes else ""

        out[slug] = {
            "buy_price": parse_price(buy_text),
            "sell_price": parse_price(sell_text),
            "updated_at": updated_time,
        }
    return out


async def fetch_seketehran_http(client: httpx.AsyncClient, url: str) -> Dict[str, Dict[str, Optional[Any]]]:
    """Fetches seketehran.ir via simple HTTP GET and parses it."""
    url = url or SEKETEHRAN_DEFAULT_URL
    logger.debug("Fetching seketehran with HTTP from: %s", url)

    async def _do_fetch():
        resp = await client.get(url, timeout=10.0)
        resp.raise_for_status()
        return resp.text

    html_text = await with_retries(_do_fetch, retries=3, backoff_base=0.5)
    return parse_seketehran_html(html_text)


async def fetch_seketehran_playwright(page: Page, url: str = SEKETEHRAN_DEFAULT_URL) -> Dict[str, Dict[str, Optional[Any]]]:
    """Scrapes prices from seketehran.ir using an existing Playwright page."""
    logger.debug("Fetching seketehran with Playwright from: %s", url)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        # Wait for the update element to appear (or at least rows)
        try:
            await page.wait_for_selector("div[class*='table_updated'] span", timeout=10000)
        except Exception:
            # fallback to rows selector if update element isn't present
            await page.wait_for_selector("div[class*='table_row']", timeout=10000)

        # short delay for final rendering
        await page.wait_for_timeout(500)

        upd_element = await page.query_selector("div[class*='table_updated']")
        updated = None
        if upd_element:
            spans = await upd_element.query_selector_all("span")
            if len(spans) >= 2:
                # Prefer last two spans as date + time
                date = (await spans[-2].inner_text()).strip()
                time_ = (await spans[-1].inner_text()).strip()
                updated = f"{date} {time_}"
            else:
                updated = (await upd_element.inner_text()).strip()

        out: Dict[str, Dict[str, Optional[Any]]] = {}
        rows = await page.query_selector_all("div[class*='table_row']")
        for r in rows:
            title_el = await r.query_selector("div[class*='table_itemTitle']")
            title = (await title_el.inner_text()).strip() if title_el else ""
            if not title:
                continue

            slug = slugify_title(title)
            if slug not in ALLOWED_SEKETEHRAN_SLUGS:
                continue

            buy_el = await r.query_selector("div[class*='table_itemBuyPriceValue']")
            sell_el = await r.query_selector("div[class*='table_itemSellPriceValue']")

            buy_text = (await buy_el.inner_text()).strip() if buy_el else ""
            sell_text = (await sell_el.inner_text()).strip() if sell_el else ""

            out[slug] = {
                "buy_price": parse_price(buy_text),
                "sell_price": parse_price(sell_text),
                "updated_at": updated,
            }
        return out

    except PWTimeout:
        logger.error("Playwright timed out waiting for selector on seketehran.ir. Page might have changed.")
        return {}
    except Exception as exc:
        logger.exception("An unexpected error occurred during Playwright fetch: %s", exc)
        return {}


# ---- Main worker loop ----
async def run_updater(
    redis_url: str, interval: int, ttl: Optional[int], key_prefix: str,
    seketehran_url: Optional[str], use_playwright_for_sek: bool
):
    logger.info("Starting updater (redis=%s interval=%ss ttl=%s prefix=%s)", redis_url, interval, ttl, key_prefix)
    logger.info(
        "SekeTehran fetcher: %s",
        "Playwright" if use_playwright_for_sek else ("HTTP" if seketehran_url else "Disabled"),
    )
    redis = Redis.from_url(redis_url, decode_responses=True)

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient())
        playwright_page: Optional[Page] = None
        browser: Optional[Browser] = None
        pw: Optional[Playwright] = None

        if use_playwright_for_sek:
            try:
                # Initialize Playwright context
                pw = await stack.enter_async_context(async_playwright())
                # Launch browser (headless)
                browser = await pw.chromium.launch(headless=True)
                # Ensure browser is closed on exit
                stack.callback(lambda: asyncio.create_task(browser.close()))
                # Create a page
                playwright_page = await browser.new_page()

                # Block unnecessary resources to drastically speed up loading
                async def _route_handler(route: Route, request: Request) -> None:
                    if request.resource_type in {"image", "stylesheet", "font"}:
                        await route.abort()
                    else:
                        await route.continue_()

                await playwright_page.route("**/*", _route_handler)
            except Exception as e:
                logger.error("Failed to initialize Playwright: %s. Disabling Playwright for this session.", e)
                playwright_page = None

        # --- Cross-platform signal handling ---
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: stop_event.set())
        except NotImplementedError:
            # Fallback for Windows
            signal.signal(signal.SIGINT, lambda s, f: stop_event.set())
            signal.signal(signal.SIGTERM, lambda s, f: stop_event.set())
        # --- End of signal handling ---

        try:
            while not stop_event.is_set():
                cycle_start = time.perf_counter()
                logger.debug("Update cycle start")

                # Build tasks and keep keys list for mapping results reliably
                task_items: List[Tuple[str, asyncio.Task]] = []
                # mohammadrahimi always requested
                task_items.append(("moh", asyncio.create_task(fetch_mohammadrahimi(client))))
                # seketehran via playable page or http if provided
                if playwright_page:
                    task_items.append(("sek", asyncio.create_task(fetch_seketehran_playwright(playwright_page, seketehran_url or SEKETEHRAN_DEFAULT_URL))))
                elif seketehran_url:
                    task_items.append(("sek", asyncio.create_task(fetch_seketehran_http(client, seketehran_url))))

                # Wait for all tasks concurrently
                tasks = [t for _, t in task_items]
                keys = [k for k, _ in task_items]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Map results back to keys
                result_map: Dict[str, Any] = {}
                for k, res in zip(keys, results):
                    if isinstance(res, Exception):
                        logger.error("Failed to fetch %s: %s", k, res)
                        result_map[k] = {}
                    else:
                        result_map[k] = res

                moh_data = result_map.get("moh", {}) or {}
                sek_data = result_map.get("sek", {}) or {}

                logger.debug("mohammadrahimi items: %d", len(moh_data))
                logger.debug("seketehran items: %d", len(sek_data))

                # Combine sources (mohammadrahimi takes precedence for matching slugs)
                combined = {**sek_data, **moh_data}

                # Ensure we only write allowed seketehran items (if source is seketehran)
                total_written = 0
                for slug, payload in combined.items():
                    # determine source: if present in moh_data -> mohammadrahimi else seketehran
                    source = "mohammadrahimi" if slug in moh_data else "seketehran"
                    # If source is seketehran and slug not in allowed set, skip (defensive)
                    if source == "seketehran" and slug not in ALLOWED_SEKETEHRAN_SLUGS:
                        continue

                    key = make_redis_key(key_prefix, source, slug)
                    minimal = {
                        "buy_price": payload.get("buy_price"),
                        "sell_price": payload.get("sell_price"),
                        "updated_at": payload.get("updated_at"),
                    }
                    try:
                        await set_redis_json(redis, key, minimal, ttl=ttl)
                        total_written += 1
                    except Exception as exc:
                        logger.exception("Failed to write key to Redis: %s", key)

                logger.info("Wrote %d keys to Redis (prefix=%s)", total_written, key_prefix)
                elapsed = time.perf_counter() - cycle_start
                to_sleep = max(0, interval - elapsed)
                if to_sleep > 0:
                    logger.debug("Sleeping %.3fs until next cycle", to_sleep)
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=to_sleep)
                    except asyncio.TimeoutError:
                        continue
        finally:
            try:
                await redis.close()
                await redis.connection_pool.disconnect()
            except Exception:
                pass
            logger.info("Updater shutting down (resources released).")


# ---- CLI Entrypoint ----
def parse_args():
    parser = argparse.ArgumentParser(description="Price updater that stores latest prices in Redis.")
    parser.add_argument("--redis", default=DEFAULT_REDIS_URL, help="Redis URL (env REDIS_URL)")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="Polling interval in seconds")
    parser.add_argument("--ttl", type=int, default=(int(DEFAULT_TTL) if DEFAULT_TTL else 0), help="Optional TTL for Redis keys in seconds (0 = no TTL)")
    parser.add_argument("--prefix", default=DEFAULT_KEY_PREFIX, help="Redis key prefix")
    parser.add_argument("--sek-url", default=os.getenv("SEKETEHRAN_URL"), help="seketehran URL to scrape with HTTP (fallback).")
    parser.add_argument("--no-playwright-for-sek", dest="use_playwright_for_sek", action="store_false", help="Disable Playwright and use HTTP instead. (Playwright is default)")
    parser.set_defaults(use_playwright_for_sek=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(run_updater(
            redis_url=args.redis,
            interval=args.interval,
            ttl=args.ttl if args.ttl > 0 else None,
            key_prefix=args.prefix,
            seketehran_url=args.sek_url,
            use_playwright_for_sek=args.use_playwright_for_sek,
        ))
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
