# Long-Term Memory

- Workspace created on 2026-03-11 for a Telegram/OpenClaw-compatible UK food price bot.
- Local OpenClaw CLI/runtime was not present during setup, so the workspace includes a standalone Telegram polling runner.
- The bot should stay narrow: route UK food price questions to the right datasets and keep product-page extraction as an exact-price fallback.
- Major built-in source families are ONS, Food Foundation, community supermarket datasets, and Geolytix store-location data.
- Direct supermarket item lookup is now Pi-safe: build `data/community_supermarket_latest.csv` locally from the Kaggle-derived parquet mirror with `scripts/build_supermarket_latest_csv.py`, then deploy the CSV with the code. The Pi runtime reads that CSV with the Python standard library and filters matches to food categories.
- On the Pi, prefer `/usr/bin/python3` over the workspace `.venv` unless the venv can import `requests`; otherwise Telegram item lookups fail before the pipeline starts. If live replies look stale after AGENTS changes, clear `agent:springfield-price:main` from the Pi session map and restart `systemctl --user openclaw-gateway.service`.
- For specialist product queries such as Wagyu beef, prefer live retailer search pages first and only fall back to `data/community_supermarket_latest.csv` when live pages do not produce good matches. Current live parsers target Tom Hixson and Fine Food Specialist Shopify search pages.
- Direct item lookup can now use a CSV-first shortlist with Bright Data Google Shopping live checks before falling back to retailer search pages and then the CSV snapshot. Bright Data is optional and only activates when `SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY` and `SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE` are present.
- Direct item lookup can also use PricesAPI ahead of Bright Data. Current order is CSV shortlist -> PricesAPI -> Bright Data -> retailer search pages -> CSV fallback. PricesAPI is optional and only activates when `SPRINGFIELD_PRICE_PRICESAPI_KEY` is present.
