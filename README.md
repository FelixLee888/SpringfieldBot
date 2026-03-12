# Springfield Price Bot

Springfield Price Bot is an OpenClaw-compatible Telegram bot workspace for UK food price intelligence.

## What It Does

- Accepts plain-English UK food price questions and routes them to the strongest matching data source.
- Checks recent cached item-level results first (24-hour default TTL) to avoid repeated crawling for the same query.
- Archives expired item-cache rows into SQLite history for longer-term price trend analysis.
- Migrates CSV snapshot rows into SQLite history tables so CSV fallback is managed from one central store.
- Uses the Kaggle-derived CSV snapshot as the final fallback when live sources cannot return a qualifying match.
- Normalizes price-per-unit comparison where possible, so results can rank on `£/kg`, `£/l`, or `£/each` instead of only shelf price.
- Attempts live item-level retailer comparisons from reachable retailer search pages or retailer JSON search endpoints first (excluding Amazon HTML scraping), including live supermarket comparisons through Trolley.
- Uses Amazon Product Advertising API next for Amazon UK offers when credentials are configured.
- Uses PricesAPI after Amazon API when earlier live stages do not yield a usable match.
- Prioritizes official UK statistics when the user asks for government or official sources.
- Highlights the tradeoff between official averages, basket trackers, and community-scraped retailer data.
- Keeps public product page URL and pasted HTML extraction as a fallback for exact current page-price lookups.
- Includes a standalone Telegram polling runner so the bot can still operate outside an OpenClaw runtime.

## Source Coverage

Primary sources wired into the routing logic:

- ONS Shopping Prices Comparison Tool
- Food Foundation Basic Basket Tracker
- ONS Online Weekly Price Changes
- Defra Food Statistics Pocketbook
- Community UK supermarket time-series dataset
- Geolytix supermarket location data

The bot treats these differently:

- Official ONS and GOV sources are best for trends, averages, and policy-grade context.
- Food Foundation is best for weekly basket affordability.
- Community retailer datasets are best for supermarket-to-supermarket value checks and price-per-unit comparisons.
- Amazon Product Advertising API is the preferred Amazon path to avoid anti-automation failures from direct page scraping.
- PricesAPI is useful for low-cost live offer checks, but its seller coverage is catalog-dependent and may not include every UK grocery retailer.
- For specialist and supermarket live-product searches, the bot can scrape reachable retailer search pages and retailer JSON search endpoints (including Trolley's supermarket comparison pages), then return current product links.
- The Pi-compatible snapshot fallback reads a locally built CSV file, so the runtime does not need parquet readers or Kaggle credentials.
- Geolytix helps find nearby stores, but it is not a price feed.
- Nearby-store queries default to postcode `PA2 0SG` when the user does not supply a location.

## Repository Layout

```text
scripts/
  springfield_price_pipeline.py   # Query routing + live search pages + Amazon API + PricesAPI + internal history lookup
  run_telegram_bot.py             # Standalone Telegram polling runner
  build_supermarket_latest_csv.py # Local parquet-to-CSV snapshot builder
tests/
  test_springfield_price_pipeline.py
  fixtures/
    bedworld_sample.html
```

## Requirements

- Python 3.10+
- `requests`
- `duckdb` only when rebuilding the supermarket CSV snapshot locally

Install runtime requirements:

```bash
python3 -m pip install --upgrade -r requirements.txt
```

Rebuild the supermarket CSV snapshot locally when needed:

```bash
python3 -m venv /tmp/springfield-price-build
/tmp/springfield-price-build/bin/pip install --upgrade pip duckdb
/tmp/springfield-price-build/bin/python scripts/build_supermarket_latest_csv.py
```

## Environment

Set secrets in `.env` and never commit them.

Required:

- `TELEGRAM_BOT_TOKEN`

Optional:

- `SPRINGFIELD_PRICE_ALLOWED_CHAT_IDS` comma-separated allow-list
- `SPRINGFIELD_PRICE_USER_AGENT`
- `SPRINGFIELD_PRICE_FETCH_TIMEOUT_SEC`
- `SPRINGFIELD_PRICE_DEFAULT_POSTCODE` location fallback for nearby-store queries, default `PA2 0SG`
- `SPRINGFIELD_PRICE_CACHE_ENABLED` enable SQLite caching for recent live fetches, default `1`
- `SPRINGFIELD_PRICE_CACHE_DB_PATH` optional SQLite cache path, default `data/search_cache.sqlite3`
- `SPRINGFIELD_PRICE_CACHE_TTL_SEC` default cache freshness window in seconds, default `14400`
- `SPRINGFIELD_PRICE_CACHE_HTML_TTL_SEC` optional HTML cache TTL override
- `SPRINGFIELD_PRICE_CACHE_JSON_TTL_SEC` optional JSON page cache TTL override
- `SPRINGFIELD_PRICE_CACHE_API_TTL_SEC` optional API cache TTL override for Amazon API and PricesAPI
- `SPRINGFIELD_PRICE_CACHE_ITEM_TTL_SEC` optional item-lookup cache TTL override, default `86400` (24 hours)
- `SPRINGFIELD_PRICE_AMAZON_API_ACCESS_KEY` Amazon Product Advertising API access key
- `SPRINGFIELD_PRICE_AMAZON_API_SECRET_KEY` Amazon Product Advertising API secret key
- `SPRINGFIELD_PRICE_AMAZON_API_PARTNER_TAG` Amazon Associates partner tag
- `SPRINGFIELD_PRICE_AMAZON_API_HOST` optional, defaults to `webservices.amazon.co.uk`
- `SPRINGFIELD_PRICE_AMAZON_API_REGION` optional, defaults to `eu-west-1`
- `SPRINGFIELD_PRICE_AMAZON_API_MARKETPLACE` optional, defaults to `www.amazon.co.uk`
- `SPRINGFIELD_PRICE_AMAZON_API_SEARCH_INDEX` optional, defaults to `All`
- `SPRINGFIELD_PRICE_PRICESAPI_KEY` PricesAPI key for live catalog/offers lookup
- `SPRINGFIELD_PRICE_PRICESAPI_COUNTRY` optional, defaults to `uk`
- `SPRINGFIELD_PRICE_TROLLEY_PRODUCT_FETCH_LIMIT` optional, defaults to `6`

The direct item lookup order is:

1. recent item-level cache hit (if available)
2. other live retailer/supermarket sources (excluding Amazon HTML scraping)
3. Amazon Product Advertising API
4. PricesAPI live offers
5. internal history records lookup (served from migrated SQLite history rows)

Current live retailer-search sources:

- Trolley UK supermarket comparison pages (including M&S where available)
- Tom Hixson
- Fine Food Specialist
- Costco UK via `rest/v2/uk/products/search`, with a product-page price fallback when the JSON search result omits price
- Wanahong UK via WooCommerce product search (`/?s=...&post_type=product`)

Recent live searches are cached in SQLite so repeated item lookups do not refetch the same retailer pages and API responses over and over again. Stale item-cache entries are archived into `price_history` before deletion, and CSV rows are migrated into the same SQLite history domain (tracked by `history_import_state`) for centralized management.

Comparison-function implementation notes:

- Keyword-first product matching, then per-retailer best-offer selection
- Standardized unit-price comparison (`£/kg`, `£/l`, `£/each`) before falling back to shelf price
- Own-brand tagging when the CSV source exposes it
- Explicit caveats about freshness and data-source quality

Reference material for the comparison design:

- [Creating a Price Comparison Site for the UK's top 5 supermarkets with Python, Github and Streamlit](https://medium.com/@decmca21/creating-a-price-comparison-site-for-the-uks-top-5-supermarkets-with-python-github-and-streamlit-bd20b6f16ff2)
- [Create a Price Comparison Site for the UK's top 5 supermarkets with Python, Github and Streamlit](https://medium.com/@decmca21/create-a-price-comparison-site-for-the-uks-top-5-supermarkets-with-python-github-and-streamlit-30ed8dca4eb4)
- [RamonWill/price-comparison-project](https://github.com/RamonWill/price-comparison-project)
- [RamonWill/price-comparison-project `SuperMarkIt/scripts/webscrapers.py`](https://github.com/RamonWill/price-comparison-project/blob/master/SuperMarkIt/scripts/webscrapers.py)

SpringfieldPriceBot keeps the same comparison ideas from those articles, but uses a different runtime:

- It keeps keyword-driven comparison and value-per-unit ranking.
- It keeps own-brand and category signals from the CSV snapshot when available.
- It does not reuse the Selenium scraping architecture directly; instead it mixes a local CSV snapshot with live retailer HTML/JSON fetches that fit the Pi deployment.
- It prefers minimal direct fetches over a full browser stack so the Pi runtime stays lightweight.
- It also borrows the `webscrapers.py` pattern of one shared fetch helper plus retailer-specific product-page extractors, used here as fallback selectors for Tesco, Sainsbury's, and Morrisons when generic JSON-LD/meta parsing misses the page price.

## Pipeline Usage

Natural-language query:

```bash
python3 scripts/springfield_price_pipeline.py "Where can I compare the best value eggs across Tesco and ASDA this week?"
```

Official-source trend query:

```bash
python3 scripts/springfield_price_pipeline.py "Show official UK food price inflation sources for milk and bread."
```

Public product page URL:

```bash
python3 scripts/springfield_price_pipeline.py "https://example.com/product"
```

Trusted local HTML file for local testing:

```bash
python3 scripts/springfield_price_pipeline.py tests/fixtures/bedworld_sample.html --json --json-brief
```

Pasted HTML:

```bash
cat product.html | python3 scripts/springfield_price_pipeline.py --stdin --json --json-brief
```

Local file-path support is disabled automatically when the Telegram runner invokes the pipeline.

## Standalone Telegram Runner

Token check:

```bash
python3 scripts/run_telegram_bot.py --check
```

Run bot:

```bash
python3 scripts/run_telegram_bot.py
```

The polling runner stores the last Telegram update offset at `data/telegram_offset.txt`.
It already loads `.env`, so Amazon API and PricesAPI credentials placed there are available to the pipeline.

## OpenClaw Integration

If OpenClaw is available on the target machine:

```bash
openclaw agents add springfield-price --workspace "/Users/felixlee/Documents/SpringfieldPriceBot"
openclaw routes add "#springfield-price" springfield-price
```

A starter config is included in `openclaw.config.example.json`.

## Suggested BotFather Text

Description:

```text
Ask me about UK food prices, inflation, basket costs, or supermarket value. I can also extract a live price from a public product page.
```

About:

```text
UK food price intelligence bot
```

## Notes

- Official sources in this bot are trend and context tools, not live checkout feeds.
- Community retailer datasets are useful for operational price comparison, but they are not official statistics.
- Amazon Product Advertising API avoids Amazon anti-automation scrape failures, but still depends on valid Associates credentials and API policies.
- PricesAPI live offer checks are also fresher than the CSV snapshot, but the API does not guarantee coverage for every UK supermarket in the shortlist.
- Live retailer search-page matches can be fresher, but they depend on each retailer exposing parseable search HTML or JSON or being reachable through Trolley-style comparison pages.
- Value ranking compares standardized unit price first when the item size is known, then falls back to shelf price.
- Telegram users can send a public product URL when they need an exact live page-price extraction.
