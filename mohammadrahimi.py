import requests
import json
import sys

url = "https://api.mohammadrahimi.com/products"
headers = {
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
    "Cookie": "access_token_web=eyJpdiI6ImhWb3ZPMHl4NDJkbkFET3RjRUJ1bEE9PSIsInZhbHVlIjoidGt4NXYyU0NrK1VSNGppdERSdlBRcGdraWdiNjhnZEhZMmRHSEI1eEdKeEozZlNHR3Ywakd0Nk1RSjhTWGRVa0FUMVpHK1VRWmYwbHdkYm1vS0x0c21PWHNNVkxwaXBFbm5KMnNnaGQ1dUNMRDFSaW5DM2hBNnZIQXpWT3RKaTdrRExreTJPRElSN0Y3UGl6MkY3bUM4cEJVWk9vVzVuWm5xUzZYWUFiekJQNUk4QlFDQ1NPakhSaWhJTkh0Q1hTRnZIVjhPNEFRakxvVjlWQjdMYmgyakZxRURqak5YYWM3ZE5VZXA0VDlHbWE4V3VsVkdEejVpWWVaMUFBN2ZPR09aZVRJN1lub2Y2L2U0WFNmTnlCYkFyMTZiWEtqeWh0MkJWMTNGbFZ4N2drdjhicjNMUUcrS2FYdjQ1eEY2bVMiLCJtYWMiOiI4NTg5ZjQ0ZDYxYjVjM2YwOTJiZjIzNTBhMDE5ZmNhZThkMWNiMjBjNWJmM2FlN2VmZjk1YzhiMzg5YmM1ZTM3IiwidGFnIjoiIn0%3D"
}

try:
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
except requests.RequestException as e:
    print("Request failed:", e, file=sys.stderr)
    sys.exit(1)

# parse JSON
try:
    payload = resp.json()
except ValueError:
    print("Response is not JSON:", resp.text, file=sys.stderr)
    sys.exit(1)

# iterate and print
items = payload.get("data", [])
if not items:
    print("No products found.")
    sys.exit(0)

for category in items:
    products = category.get("products", [])
    for p in products:
        title = p.get("title", "<no title>")
        buy_price = p.get("buy_price", "<no buy_price>")
        sell_price = p.get("sell_price", "<no sell_price>")
        updated_at = p.get("updated_at", "<no updated_at>")
        # clean up sentinel values like -1 if you want:
        if buy_price == -1:
            buy_price = "<not available>"
        if sell_price == -1:
            sell_price = "<not available>"

        print(f"{title}  |  buy_price: {buy_price}  |  sell_price: {sell_price}  |  updated_at: {updated_at}")
