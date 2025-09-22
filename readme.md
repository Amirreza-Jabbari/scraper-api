python scraper.py --interval 60 --ttl 120

uvicorn api:app --host 0.0.0.0 --port 8000 --log-level info

python dump_prices.py