# Springfield Price Bot

Springfield Price Bot is an OpenClaw-compatible Telegram bot workspace for UK food price intelligence.

## What It Does

- Accepts plain-English UK food price questions and routes them to the strongest matching data source.
- Uses the Kaggle-derived CSV snapshot first for item matching and retailer shortlisting.
- Attempts live item-level retailer comparisons from reachable retailer search pages next for specialist products such as Wagyu beef.
- Uses PricesAPI after retailer search pages when credentials are configured and the live retailer-page path does not yield a usable match.
- Keeps Bright Data Google Shopping as a later live-offer path when PricesAPI is unavailable or does not return usable matches.
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
- PricesAPI is useful for low-cost live offer checks, but its seller coverage is catalog-dependent and may not include every UK grocery retailer.
- Bright Data Google Shopping is best for fresher live offer checks once the CSV shortlist identifies which retailers are worth probing.
- For specialist live-product searches, the bot can also scrape reachable retailer search pages and return current product links.
- The Pi-compatible snapshot fallback reads a locally built CSV file, so the runtime does not need parquet readers or Kaggle credentials.
- Geolytix helps find nearby stores, but it is not a price feed.
- Nearby-store queries default to postcode `PA2 0SG` when the user does not supply a location.

## Repository Layout

```text
scripts/
  springfield_price_pipeline.py   # Query routing + CSV + live search page + PricesAPI/Bright Data lookup
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
- `SPRINGFIELD_PRICE_PRICESAPI_KEY` PricesAPI key for live catalog/offers lookup
- `SPRINGFIELD_PRICE_PRICESAPI_COUNTRY` optional, defaults to `uk`
- `SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY` Bright Data API token for live Google Shopping lookups
- `SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE` Bright Data SERP API zone name
- `SPRINGFIELD_PRICE_BRIGHTDATA_SERP_COUNTRY` optional, defaults to `UK`
- `SPRINGFIELD_PRICE_BRIGHTDATA_SERP_HOST` optional, defaults to `www.google.com`
- `SPRINGFIELD_PRICE_BRIGHTDATA_SERP_LANGUAGE` optional, defaults to `en`
- `SPRINGFIELD_PRICE_BRIGHTDATA_SERP_GEO` optional, defaults to `gb`

The direct item lookup order is:

1. Kaggle-derived CSV snapshot shortlist
2. Reachable retailer search pages
3. PricesAPI live offers for the top CSV retailers when configured
4. Bright Data Google Shopping live offers for the top CSV retailers when configured
5. CSV snapshot fallback

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
It already loads `.env`, so Bright Data credentials placed there are available to the pipeline.

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
- Bright Data live offer checks are fresher than the CSV snapshot, but they depend on Google Shopping and merchant-feed coverage.
- PricesAPI live offer checks are also fresher than the CSV snapshot, but the API does not guarantee coverage for every UK supermarket in the shortlist.
- Live retailer search-page matches can be fresher, but they depend on each retailer exposing parseable search HTML.
- Telegram users can send a public product URL when they need an exact live page-price extraction.
