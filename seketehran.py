# seketehran_playwright.py
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.seketehran.ir/"

def clean_num(s: str) -> str:
    """Cleans and converts Persian/Arabic numerals to English digits."""
    if not s:
        return ""
    s = s.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789"))
    return re.sub(r"[^\d]", "", s)

def scrape_playwright(url=URL, headless=True, wait_ms=10000, page_timeout=20000):
    """Scrapes gold and coin prices from seketehran.ir using Playwright."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        # Block unnecessary resources to drastically speed up loading
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image", "stylesheet", "font"} else route.continue_())

        try:
            # Go to the page and wait for the initial HTML to be ready
            page.goto(url, wait_until="domcontentloaded", timeout=page_timeout)

            # --- KEY CHANGE: HYBRID WAIT STRATEGY ---
            # 1. Wait for the timestamp to appear, signaling the main data is loaded.
            page.wait_for_selector("div[class*='table_updated'] span:nth-child(2)", timeout=wait_ms)
            
            # 2. Add a short, fixed delay to allow the final prices to render.
            page.wait_for_timeout(500) # 500 milliseconds

            # Get the "last updated" timestamp
            upd_element = page.query_selector("div[class*='table_updated']")
            updated = ""
            if upd_element:
                spans = upd_element.query_selector_all("span")
                if len(spans) >= 3:
                    date = spans[1].inner_text().strip()
                    time_ = spans[2].inner_text().strip()
                    updated = f"{date} {time_}"
                else:
                    updated = upd_element.inner_text().strip()

            items = []
            rows = page.query_selector_all("div[class*='table_row']")
            for r in rows:
                title_el = r.query_selector("div[class*='table_itemTitle']")
                buy_el = r.query_selector("div[class*='table_itemBuyPriceValue']")
                sell_el = r.query_selector("div[class*='table_itemSellPriceValue']")

                if not (title_el and buy_el and sell_el):
                    continue
                
                title = title_el.inner_text().strip()
                buy = clean_num(buy_el.inner_text())
                sell = clean_num(sell_el.inner_text())
                
                # Only add rows that have a title
                if title:
                    items.append({
                        "title": title,
                        "buy_price": buy or "0",
                        "sell_price": sell or "0",
                        "updated_at": updated or None
                    })

        except PWTimeout:
            print(f"Timed out after {wait_ms/1000}s. The website might be slow or its structure has changed.")
            return []
        finally:
            browser.close()
            
        return items

if __name__ == "__main__":
    rows = scrape_playwright()
    if not rows:
        print("No items found. Please check your connection or the website's status.")
    else:
        print(f"{'Item':<25} | {'Buy Price':>15} | {'Sell Price':>15} | {'Updated At'}")
        print("-" * 80)
        for it in rows:
            print(f"{it['title']:<25} | {it['buy_price']:>15} | {it['sell_price']:>15} | {it.get('updated_at', 'N/A')}")