#!/usr/bin/env python3
"""Analyze UK food price questions or extract product pricing from product pages."""

from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import html
import ipaddress
import json
import math
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests

PRICESAPI_DOCS_URL = "https://pricesapi.io/docs"
PRICESAPI_BASE_URL = os.getenv("SPRINGFIELD_PRICE_PRICESAPI_BASE_URL", "https://api.pricesapi.io/api/v1").strip() or "https://api.pricesapi.io/api/v1"
PRICESAPI_COUNTRY = os.getenv("SPRINGFIELD_PRICE_PRICESAPI_COUNTRY", "uk").strip().lower() or "uk"
PRICESAPI_SEARCH_LIMIT = int(os.getenv("SPRINGFIELD_PRICE_PRICESAPI_SEARCH_LIMIT", "5"))
PRICESAPI_PRODUCT_LIMIT = int(os.getenv("SPRINGFIELD_PRICE_PRICESAPI_PRODUCT_LIMIT", "3"))
AMAZON_PAAPI_DOCS_URL = "https://webservices.amazon.com/paapi5/documentation/"
AMAZON_PAAPI_HOST = os.getenv("SPRINGFIELD_PRICE_AMAZON_API_HOST", "webservices.amazon.co.uk").strip() or "webservices.amazon.co.uk"
AMAZON_PAAPI_REGION = os.getenv("SPRINGFIELD_PRICE_AMAZON_API_REGION", "eu-west-1").strip() or "eu-west-1"
AMAZON_PAAPI_MARKETPLACE = os.getenv("SPRINGFIELD_PRICE_AMAZON_API_MARKETPLACE", "www.amazon.co.uk").strip() or "www.amazon.co.uk"
AMAZON_PAAPI_PARTNER_TYPE = os.getenv("SPRINGFIELD_PRICE_AMAZON_API_PARTNER_TYPE", "Associates").strip() or "Associates"
AMAZON_PAAPI_SEARCH_INDEX = os.getenv("SPRINGFIELD_PRICE_AMAZON_API_SEARCH_INDEX", "All").strip() or "All"
AMAZON_PAAPI_ITEM_LIMIT = int(os.getenv("SPRINGFIELD_PRICE_AMAZON_API_ITEM_LIMIT", "3"))
BRIGHTDATA_GOOGLE_SHOPPING_DOCS_URL = "https://docs.brightdata.com/api-reference/serp/google-search/shopping"
BRIGHTDATA_SERP_ENDPOINT = os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ENDPOINT", "https://api.brightdata.com/request").strip() or "https://api.brightdata.com/request"
BRIGHTDATA_SERP_COUNTRY = os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_COUNTRY", "UK").strip() or "UK"
BRIGHTDATA_SERP_HOST = os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_HOST", "www.google.com").strip() or "www.google.com"
BRIGHTDATA_SERP_LANGUAGE = os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_LANGUAGE", "en").strip() or "en"
BRIGHTDATA_SERP_GEO = os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_GEO", "gb").strip() or "gb"
TROLLEY_BASE_URL = "https://www.trolley.co.uk/"
DEFAULT_USER_AGENT = os.getenv(
    "SPRINGFIELD_PRICE_USER_AGENT",
    "Mozilla/5.0 (compatible; SpringfieldPriceBot/2.0; +https://t.me/Springfield_Price_Bot)",
)
DEFAULT_TIMEOUT = float(os.getenv("SPRINGFIELD_PRICE_FETCH_TIMEOUT_SEC", "20"))
DEFAULT_LOCATION_POSTCODE = os.getenv("SPRINGFIELD_PRICE_DEFAULT_POSTCODE", "PA2 0SG").strip() or "PA2 0SG"
CACHE_DB_DEFAULT_PATH = Path(__file__).resolve().parent.parent / "data" / "search_cache.sqlite3"
CACHE_DEFAULT_TTL_SEC = int(os.getenv("SPRINGFIELD_PRICE_CACHE_TTL_SEC", "14400"))
ITEM_CACHE_DEFAULT_TTL_SEC = 24 * 60 * 60
URL_RE = re.compile(r"https?://[^\s<>'\"]+")
WORD_RE = re.compile(r"[a-z0-9']+")
UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", re.IGNORECASE)
SCRIPT_JSONLD_RE = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
DATA_VARIATIONS_RE = re.compile(
    r"data-product_variations=(['\"])(.*?)\1",
    re.IGNORECASE | re.DOTALL,
)
META_PRICE_RE = re.compile(
    r"<meta[^>]+(?:property|name)=[\"'](?:product:price:amount|og:price:amount|twitter:data1)[\"'][^>]+content=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
META_CURRENCY_RE = re.compile(
    r"<meta[^>]+(?:property|name)=[\"'](?:product:price:currency|og:price:currency)[\"'][^>]+content=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
CLASS_BLOCK_RE_TEMPLATE = r"<(?P<tag>[a-z0-9]+)[^>]+class=[\"'][^\"']*\b{class_name}\b[^\"']*[\"'][^>]*>(?P<body>.*?)</(?P=tag)>"
WHITESPACE_RE = re.compile(r"\s+")
AMOUNT_RE = re.compile(r"(?:£|\$|€)\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)")
PRICE_PER_STANDARD_UNIT_RE = re.compile(
    r"(?:£|\$|€)\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)\s*(?:/|per\s*)(?:(\d+(?:\.\d+)?)\s*)?(kg|g|oz|lb|l|litre|liter|ml|cl|each|ea|unit)\b",
    re.IGNORECASE,
)
MULTIPACK_MEASURE_RE = re.compile(
    r"\b(\d+)\s*[xX]\s*(\d+(?:\.\d+)?)\s*(kg|g|oz|lb|l|litre|liter|ml|cl)\b",
    re.IGNORECASE,
)
SINGLE_MEASURE_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(kg|g|oz|lb|l|litre|liter|ml|cl)\b", re.IGNORECASE)
COUNT_PACK_RE = re.compile(r"\b(?:pack\s+of\s+)?(\d+)\s*(?:pack|packs|pk|pcs?|pieces?|eggs?|bottles?|cans?)\b", re.IGNORECASE)
COUNT_TAIL_RE = re.compile(r"\bx\s*(\d+)\s*$", re.IGNORECASE)
TEXT_AVAILABILITY_RE = re.compile(r"\b(in stock|out of stock|available|unavailable)\b", re.IGNORECASE)
TROLLEY_PRODUCT_LINK_RE = re.compile(r'href="(/product/[^"]+)"')
TROLLEY_BRAND_RE = re.compile(r'<div class="_brand">([^<]+)</div>', re.IGNORECASE)
TROLLEY_DESC_RE = re.compile(r'<div class="_desc">([^<]+)</div>', re.IGNORECASE)
TROLLEY_OFFER_RE = re.compile(
    r'<div class="_item">.*?class="store-logo\s+-([^"\s]+)[^"]*".*?<div class="_price">\s*<b>&pound;([0-9]+(?:\.[0-9]{1,2})?)</b>',
    re.IGNORECASE | re.DOTALL,
)
WANAHONG_PRODUCT_CARD_RE = re.compile(r'<article class="product-summary[^"]*".*?</article>', re.IGNORECASE | re.DOTALL)
WANAHONG_NAME_RE = re.compile(r'<h5 class="product-summary-name">(.*?)</h5>', re.IGNORECASE | re.DOTALL)
WANAHONG_LINK_RE = re.compile(r'<a href="([^"]+)"')
WANAHONG_PRICE_RE = re.compile(r'<div class="product-summary-price">(.*?)</div>', re.IGNORECASE | re.DOTALL)
WANAHONG_INS_PRICE_RE = re.compile(r"<ins[^>]*>(.*?)</ins>", re.IGNORECASE | re.DOTALL)
LOCAL_FILE_FALSE_VALUES = {"0", "false", "no", "off"}
_CACHE_DISABLED_VALUES = {"0", "false", "no", "off"}
_CACHE_NAMESPACE_TTLS = {
    "html_url": "SPRINGFIELD_PRICE_CACHE_HTML_TTL_SEC",
    "json_url": "SPRINGFIELD_PRICE_CACHE_JSON_TTL_SEC",
    "pricesapi_json": "SPRINGFIELD_PRICE_CACHE_API_TTL_SEC",
    "amazon_paapi_json": "SPRINGFIELD_PRICE_CACHE_API_TTL_SEC",
    "brightdata_shopping": "SPRINGFIELD_PRICE_CACHE_API_TTL_SEC",
    "item_lookup_result": "SPRINGFIELD_PRICE_CACHE_ITEM_TTL_SEC",
}
_CACHE_NAMESPACE_DEFAULT_TTLS = {
    "item_lookup_result": ITEM_CACHE_DEFAULT_TTL_SEC,
}
CSV_HISTORY_DATASET_PREFIX = "community_supermarket_csv::"

QUERY_LABELS = {
    "retailer_comparison": "Retailer comparison",
    "basket_affordability": "Basket affordability",
    "inflation_trend": "Inflation or trend analysis",
    "store_location": "Store location support",
    "item_price_lookup": "Item-level price lookup",
}

RETAILER_ALIASES = {
    "aldi": "Aldi",
    "amazon": "Amazon UK",
    "amazonuk": "Amazon UK",
    "asda": "ASDA",
    "coop": "Co-op",
    "co-op": "Co-op",
    "costco": "Costco",
    "iceland": "Iceland",
    "lidl": "Lidl",
    "mands": "M&S",
    "marksandspencer": "M&S",
    "morrisons": "Morrisons",
    "ocado": "Ocado",
    "sainsbury": "Sainsbury's",
    "sainsburys": "Sainsbury's",
    "tesco": "Tesco",
    "wanahong": "Wanahong",
    "waitrose": "Waitrose",
}
BASKET_KEYWORDS = {"basket", "weekly", "shop", "shopping", "affordability", "affordable", "nutritious", "groceries", "grocery"}
TREND_KEYWORDS = {"inflation", "trend", "trends", "history", "historical", "rise", "rising", "fall", "falling", "over", "time", "change", "changes", "year", "monthly", "weekly"}
LOCATION_KEYWORDS = {"near", "nearby", "postcode", "location", "locations", "store", "stores", "branch", "branches", "map"}
VALUE_KEYWORDS = {"best", "value", "cheapest", "cheap", "lowest", "compare", "comparison", "versus", "vs", "unit", "price", "prices", "per", "deal"}
LIVE_PRICE_KEYWORDS = {"today", "current", "currently", "live", "now", "this", "week"}
OFFICIAL_ONLY_KEYWORDS = {"official", "government", "ons", "gov", "govuk", "defra"}
PACKAGE_QUERY_TERMS = {"bag", "bags", "bottle", "bottles", "box", "boxes", "carton", "cartons", "case", "cases", "pack", "packs", "packet", "packets", "tray", "trays"}
EGG_QUERY_EXCLUDED_TERMS = {"cadbury", "chocolate", "creme", "duck", "easter", "mini", "quail", "scotch", "savoury", "surprise"}
QUERY_NOISE = {
    "a",
    "an",
    "and",
    "across",
    "are",
    "am",
    "before",
    "buy",
    "best",
    "can",
    "compare",
    "comparison",
    "cost",
    "current",
    "data",
    "dinner",
    "checking",
    "cook",
    "cooking",
    "find",
    "food",
    "for",
    "help",
    "how",
    "i",
    "in",
    "is",
    "item",
    "items",
    "look",
    "lookup",
    "me",
    "of",
    "only",
    "official",
    "on",
    "prepare",
    "preparing",
    "price",
    "prices",
    "retail",
    "retailer",
    "retailers",
    "show",
    "source",
    "sources",
    "supermarket",
    "supermarkets",
    "the",
    "this",
    "to",
    "breakfast",
    "lunch",
    "meal",
    "meals",
    "tonight",
    "want",
    "uk",
    "up",
    "value",
    "week",
    "where",
    "what",
    "which",
    "with",
    *PACKAGE_QUERY_TERMS,
    "dozen",
}


@dataclass(frozen=True)
class FoodPriceSource:
    key: str
    name: str
    url: str
    description: str
    best_for: Tuple[str, ...]
    official: bool = False
    historical_only: bool = False
    retailer_level: bool = False
    location_only: bool = False
    caveat: str = ""


@dataclass
class SourcePayload:
    kind: str
    source_label: str
    content: str
    canonical_url: str = ""


@dataclass
class PriceResult:
    ok: bool
    summary: str
    source: str
    product_name: str = ""
    canonical_url: str = ""
    currency: str = ""
    current_price: Optional[float] = None
    regular_price: Optional[float] = None
    low_price: Optional[float] = None
    high_price: Optional[float] = None
    regular_low_price: Optional[float] = None
    regular_high_price: Optional[float] = None
    availability: str = ""
    discount_percent: Optional[float] = None
    error_message: str = ""
    reply_message: str = ""
    mode: str = ""
    query_type: str = ""
    matched_sources: List[str] = field(default_factory=list)


FOOD_PRICE_SOURCES: Tuple[FoodPriceSource, ...] = (
    FoodPriceSource(
        key="community_supermarket_dataset",
        name="Community UK supermarket time-series dataset",
        url="https://huggingface.co/datasets/Rif-SQL/time-series-uk-retail-supermarket-price-data",
        description="Retailer-level scraped prices and price-per-unit coverage across Aldi, ASDA, Morrisons, Tesco, and Sainsbury's, mirrored from the original Kaggle time-series dataset.",
        best_for=("retailer_comparison", "item_price_lookup"),
        retailer_level=True,
        caveat="Kaggle-derived community-scraped data rather than an official publication; freshness depends on the maintainer.",
    ),
    FoodPriceSource(
        key="ons_shopping_tool",
        name="ONS Shopping Prices Comparison Tool",
        url="https://www.ons.gov.uk/economy/inflationandpriceindices/articles/shoppingpricescomparisontool/2023-05-03",
        description="Official ONS comparison tool for average tracked prices across hundreds of consumer basket items over time.",
        best_for=("inflation_trend", "item_price_lookup"),
        official=True,
        caveat="Tracks average item prices over time rather than live shelf prices from a store checkout page.",
    ),
    FoodPriceSource(
        key="food_foundation_basic_basket",
        name="Food Foundation Basic Basket Tracker",
        url="https://foodfoundation.org.uk/initiatives/food-prices-tracking",
        description="Weekly tracker for the cost of a nutritious basic basket for households under pressure.",
        best_for=("basket_affordability", "inflation_trend"),
        caveat="Best for basket affordability and weekly pressure, not single-product live shelf comparison.",
    ),
    FoodPriceSource(
        key="ons_online_weekly_changes",
        name="ONS Online Weekly Price Changes",
        url="https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/onlineweeklypricechanges",
        description="Experimental ONS dataset of weekly online food and drink price changes scraped from major retailers.",
        best_for=("inflation_trend", "retailer_comparison"),
        official=True,
        historical_only=True,
        caveat="Useful for historical weekly movements, but the experimental series stops in 2021.",
    ),
    FoodPriceSource(
        key="gov_food_statistics",
        name="Defra Food Statistics Pocketbook",
        url="https://www.gov.uk/government/statistics/food-statistics-pocketbook/food-statistics-in-your-pocket",
        description="Official annual context on food prices, expenditure, consumption, and household food economics.",
        best_for=("inflation_trend", "basket_affordability"),
        official=True,
        historical_only=True,
        caveat="Annual macro context only, so it is not useful for live product-level comparison.",
    ),
    FoodPriceSource(
        key="geolytix_locations",
        name="Geolytix supermarket location data",
        url="https://geolytix.com/",
        description="Open retail location coverage that helps identify nearby supermarkets before checking their prices elsewhere.",
        best_for=("store_location",),
        retailer_level=True,
        location_only=True,
        caveat="Location metadata only; it does not publish food prices.",
    ),
)

SOURCE_MAP = {source.key: source for source in FOOD_PRICE_SOURCES}
COMMUNITY_DATASET_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "community_supermarket_latest.csv"
COMMUNITY_LOOKUP_MAX_OFFERS = int(os.getenv("SPRINGFIELD_PRICE_DIRECT_MATCH_LIMIT", "5"))
FOOD_CATEGORY_ALLOWLIST = {"bakery", "drinks", "food_cupboard", "free-from", "fresh_food", "frozen"}
BROAD_ITEM_TERMS = {
    "beef",
    "bread",
    "butter",
    "cheese",
    "chicken",
    "eggs",
    "fruit",
    "lamb",
    "meat",
    "milk",
    "pork",
    "rice",
    "veg",
    "vegetable",
    "vegetables",
}

DATASET_RETAILER_NAMES = {
    "aldi": "Aldi",
    "asda": "ASDA",
    "morrisons": "Morrisons",
    "sains": "Sainsbury's",
    "tesco": "Tesco",
}
CANONICAL_RETAILER_NAMES = {
    "aldi": "Aldi",
    "aldiuk": "Aldi",
    "amazon": "Amazon UK",
    "amazoncouk": "Amazon UK",
    "amazonuk": "Amazon UK",
    "asda": "ASDA",
    "asdagroceries": "ASDA",
    "coop": "Co-op",
    "cooperative": "Co-op",
    "co-op": "Co-op",
    "costco": "Costco",
    "groceriesasda": "ASDA",
    "groceriesmorrisons": "Morrisons",
    "iceland": "Iceland",
    "lidl": "Lidl",
    "morrisons": "Morrisons",
    "ocado": "Ocado",
    "sains": "Sainsbury's",
    "sainsburys": "Sainsbury's",
    "tesco": "Tesco",
    "tescogroceries": "Tesco",
    "marksandspencer": "M&S",
    "mands": "M&S",
    "morrisonsdaily": "Morrisons",
    "waitrose": "Waitrose",
}
TROLLEY_STORE_CLASS_MAP = {
    "aldi": "Aldi",
    "asda": "ASDA",
    "coop": "Co-op",
    "co-op": "Co-op",
    "iceland": "Iceland",
    "lidl": "Lidl",
    "mands": "M&S",
    "marksandspencer": "M&S",
    "morrisons": "Morrisons",
    "morrisonsdaily": "Morrisons",
    "ocado": "Ocado",
    "sainsburys": "Sainsbury's",
    "tesco": "Tesco",
    "wanahong": "Wanahong",
    "waitrose": "Waitrose",
}
RETAILER_HOST_NAMES = {
    "aldi.co.uk": "Aldi",
    "amazon.co.uk": "Amazon UK",
    "costco.co.uk": "Costco",
    "groceries.asda.com": "ASDA",
    "marksandspencer.com": "M&S",
    "morrisons.com": "Morrisons",
    "groceries.morrisons.com": "Morrisons",
    "sainsburys.co.uk": "Sainsbury's",
    "tesco.com": "Tesco",
    "wanahong.co.uk": "Wanahong",
    "waitrose.com": "Waitrose",
}
MERCHANT_SEARCH_SOURCES = (
    {
        "name": "Trolley",
        "search_url": "https://www.trolley.co.uk/search/?from=search&q={query}",
        "product_base_url": "https://www.trolley.co.uk/",
        "parser": "trolley_search_html",
        "allow_broad_terms": True,
        "supports_retailer_lookup": True,
    },
    {
        "name": "Tom Hixson",
        "search_url": "https://tomhixson.co.uk/search?q={query}&options%5Bprefix%5D=last&type=product",
        "product_base_url": "https://tomhixson.co.uk/products/",
        "parser": "wlfdn_shopify",
        "allow_broad_terms": False,
    },
    {
        "name": "Fine Food Specialist",
        "search_url": "https://www.finefoodspecialist.co.uk/search?q={query}&type=product",
        "product_base_url": "https://www.finefoodspecialist.co.uk/products/",
        "parser": "shopify_meta",
        "allow_broad_terms": False,
    },
    {
        "name": "Costco",
        "retailer": "Costco",
        "search_url": "https://www.costco.co.uk/rest/v2/uk/products/search?query={query}&fields=FULL",
        "product_base_url": "https://www.costco.co.uk/",
        "parser": "costco_rest_json",
        "response_type": "json",
        "min_search_terms": 1,
        "allow_broad_terms": False,
    },
    {
        "name": "Wanahong",
        "retailer": "Wanahong",
        "search_url": "https://www.wanahong.co.uk/?s={query}&post_type=product",
        "product_base_url": "https://www.wanahong.co.uk/",
        "parser": "wanahong_woocommerce",
        "allow_broad_terms": False,
    },
)
WLFDN_PRODUCT_PUSH_RE = re.compile(r'_WLFDN\.shopify\.product_data\.push\((\{.*?\})\);', re.DOTALL)
WLFDN_HANDLE_RE = re.compile(r'"handle"\s*:\s*"([^"]+)"')
WLFDN_NAME_RE = re.compile(r'"item_name"\s*:\s*"([^"]+)"')
WLFDN_PRICE_RE = re.compile(r'"price"\s*:\s*"([^"]+)"')
MERCHANT_EXCLUDED_TERMS = {"box", "bundle", "dripping", "fat", "gin", "hamper", "pie", "sauce", "seasoning"}
MERCHANT_PRODUCT_PAGE_FETCH_LIMIT = max(1, int(os.getenv("SPRINGFIELD_PRICE_MERCHANT_PAGE_FETCH_LIMIT", "4")))
TROLLEY_PRODUCT_FETCH_LIMIT = max(1, int(os.getenv("SPRINGFIELD_PRICE_TROLLEY_PRODUCT_FETCH_LIMIT", "6")))


def read_stdin() -> str:
    return sys.stdin.read()


def local_files_allowed() -> bool:
    raw = os.getenv("SPRINGFIELD_PRICE_ALLOW_LOCAL_FILES", "1").strip().lower()
    return raw not in LOCAL_FILE_FALSE_VALUES


def cache_enabled() -> bool:
    raw = os.getenv("SPRINGFIELD_PRICE_CACHE_ENABLED", "1").strip().lower()
    return raw not in _CACHE_DISABLED_VALUES


def cache_db_path() -> Path:
    raw = os.getenv("SPRINGFIELD_PRICE_CACHE_DB_PATH", "").strip()
    if not raw:
        return CACHE_DB_DEFAULT_PATH
    return Path(raw).expanduser()


def cache_ttl_seconds(namespace: str) -> int:
    env_key = _CACHE_NAMESPACE_TTLS.get(namespace, "SPRINGFIELD_PRICE_CACHE_TTL_SEC")
    raw = os.getenv(env_key, "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            return _CACHE_NAMESPACE_DEFAULT_TTLS.get(namespace, CACHE_DEFAULT_TTL_SEC)
    return _CACHE_NAMESPACE_DEFAULT_TTLS.get(namespace, CACHE_DEFAULT_TTL_SEC)


def stable_cache_key(namespace: str, payload: Dict[str, Any]) -> Tuple[str, str]:
    fingerprint = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(f"{namespace}:{fingerprint}".encode("utf-8")).hexdigest()
    return digest, fingerprint


def open_cache_db() -> sqlite3.Connection:
    path = cache_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS request_cache (
            namespace TEXT NOT NULL,
            cache_key TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL,
            response_url TEXT NOT NULL DEFAULT '',
            body_text TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            last_accessed_at INTEGER NOT NULL,
            hit_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (namespace, cache_key)
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_request_cache_expires_at ON request_cache (expires_at)")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT NOT NULL,
            dataset_key TEXT NOT NULL DEFAULT '',
            dataset_fingerprint TEXT NOT NULL DEFAULT '',
            namespace TEXT NOT NULL DEFAULT '',
            cache_key TEXT NOT NULL DEFAULT '',
            request_fingerprint TEXT NOT NULL DEFAULT '',
            retailer TEXT NOT NULL DEFAULT '',
            product_name TEXT NOT NULL DEFAULT '',
            normalized_product_name TEXT NOT NULL DEFAULT '',
            price_gbp REAL,
            price_unit_gbp REAL,
            unit TEXT NOT NULL DEFAULT '',
            capture_date TEXT NOT NULL DEFAULT '',
            category_name TEXT NOT NULL DEFAULT '',
            is_own_brand INTEGER NOT NULL DEFAULT 0,
            product_url TEXT NOT NULL DEFAULT '',
            source_key TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL DEFAULT '',
            source_url TEXT NOT NULL DEFAULT '',
            fetched_at INTEGER NOT NULL DEFAULT 0,
            expires_at INTEGER NOT NULL DEFAULT 0,
            observed_at INTEGER NOT NULL DEFAULT 0,
            record_hash TEXT NOT NULL UNIQUE,
            raw_offer_json TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_price_history_origin_dataset ON price_history (origin, dataset_key, dataset_fingerprint)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_price_history_name ON price_history (normalized_product_name, retailer)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS history_import_state (
            dataset_key TEXT PRIMARY KEY,
            source_path TEXT NOT NULL DEFAULT '',
            source_fingerprint TEXT NOT NULL DEFAULT '',
            row_count INTEGER NOT NULL DEFAULT 0,
            imported_at INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    return connection


def parse_cached_item_lookup_offers(body_text: str, *, mark_cache_hit: bool = False) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(body_text)
    except ValueError:
        return []
    offers = payload.get("offers")
    if not isinstance(offers, list):
        return []
    restored: List[Dict[str, Any]] = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        restored_offer = dict(offer)
        if mark_cache_hit:
            restored_offer["cache_hit"] = True
        restored.append(restored_offer)
    return restored


def archive_item_lookup_cache_row(connection: sqlite3.Connection, row: sqlite3.Row, archived_at: int) -> str:
    offers = parse_cached_item_lookup_offers(str(row["body_text"] or ""))
    if not offers:
        return ""
    source_key = str(offers[0].get("lookup_source_key") or "")
    source_name = str(offers[0].get("lookup_source_name") or "")
    source_url = str(offers[0].get("lookup_source_url") or row["response_url"] or "")
    metadata_base = {
        "cache_namespace": str(row["namespace"] or ""),
        "cache_key": str(row["cache_key"] or ""),
        "request_fingerprint": str(row["request_fingerprint"] or ""),
    }
    for index, offer in enumerate(offers):
        offer_json = json.dumps(offer, sort_keys=True, ensure_ascii=True)
        record_hash = hashlib.sha256(
            f"item_cache_stale:{row['cache_key']}:{row['expires_at']}:{index}:{offer_json}".encode("utf-8")
        ).hexdigest()
        product_name = str(offer.get("product_name") or "").strip()
        retailer = retailer_display_name(str(offer.get("retailer") or "").strip())
        connection.execute(
            """
            INSERT OR IGNORE INTO price_history (
                origin,
                namespace,
                cache_key,
                request_fingerprint,
                retailer,
                product_name,
                normalized_product_name,
                price_gbp,
                price_unit_gbp,
                unit,
                capture_date,
                category_name,
                is_own_brand,
                product_url,
                source_key,
                source_name,
                source_url,
                fetched_at,
                expires_at,
                observed_at,
                record_hash,
                raw_offer_json,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "item_cache_stale",
                str(row["namespace"] or ""),
                str(row["cache_key"] or ""),
                str(row["request_fingerprint"] or ""),
                retailer,
                product_name,
                normalize_text(product_name),
                parse_amount(offer.get("price_gbp")),
                parse_amount(offer.get("price_unit_gbp")),
                str(offer.get("unit") or "").strip(),
                str(offer.get("capture_date") or "").strip(),
                str(offer.get("category_name") or "").strip().lower(),
                1 if str(offer.get("is_own_brand") or "").strip().lower() in {"1", "true", "yes"} else 0,
                str(offer.get("product_url") or "").strip(),
                str(offer.get("lookup_source_key") or source_key),
                str(offer.get("lookup_source_name") or source_name),
                str(offer.get("lookup_source_url") or source_url),
                int(row["fetched_at"] or 0),
                int(row["expires_at"] or 0),
                archived_at,
                record_hash,
                offer_json,
                json.dumps(metadata_base, sort_keys=True),
            ),
        )
    return source_key


def purge_expired_cache_rows(
    connection: sqlite3.Connection,
    now: int,
    *,
    skip_entry: Optional[Tuple[str, str]] = None,
) -> None:
    rows = connection.execute(
        """
        SELECT namespace, cache_key, request_fingerprint, response_url, body_text, fetched_at, expires_at
        FROM request_cache
        WHERE expires_at <= ?
        """,
        (now,),
    ).fetchall()
    for row in rows:
        namespace = str(row["namespace"] or "")
        cache_key = str(row["cache_key"] or "")
        if skip_entry and (namespace, cache_key) == skip_entry:
            continue
        if namespace == "item_lookup_result":
            archive_item_lookup_cache_row(connection, row, now)
        connection.execute(
            "DELETE FROM request_cache WHERE namespace = ? AND cache_key = ?",
            (namespace, cache_key),
        )


def cache_get(namespace: str, payload: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    if not cache_enabled():
        return None
    cache_key, _ = stable_cache_key(namespace, payload)
    now = int(time.time())
    connection = open_cache_db()
    try:
        purge_expired_cache_rows(connection, now)
        row = connection.execute(
            """
            SELECT body_text, response_url
            FROM request_cache
            WHERE namespace = ? AND cache_key = ? AND expires_at > ?
            """,
            (namespace, cache_key, now),
        ).fetchone()
        if row is None:
            connection.commit()
            return None
        connection.execute(
            """
            UPDATE request_cache
            SET last_accessed_at = ?, hit_count = hit_count + 1
            WHERE namespace = ? AND cache_key = ?
            """,
            (now, namespace, cache_key),
        )
        connection.commit()
        return str(row["body_text"]), str(row["response_url"])
    finally:
        connection.close()


def cache_put(namespace: str, payload: Dict[str, Any], body_text: str, response_url: str, *, ttl_seconds: Optional[int] = None) -> None:
    if not cache_enabled():
        return
    effective_ttl = cache_ttl_seconds(namespace) if ttl_seconds is None else max(0, int(ttl_seconds))
    if effective_ttl <= 0:
        return
    cache_key, fingerprint = stable_cache_key(namespace, payload)
    now = int(time.time())
    connection = open_cache_db()
    try:
        connection.execute(
            """
            INSERT INTO request_cache (
                namespace,
                cache_key,
                request_fingerprint,
                response_url,
                body_text,
                fetched_at,
                expires_at,
                last_accessed_at,
                hit_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(namespace, cache_key) DO UPDATE SET
                request_fingerprint = excluded.request_fingerprint,
                response_url = excluded.response_url,
                body_text = excluded.body_text,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at,
                last_accessed_at = excluded.last_accessed_at
            """,
            (
                namespace,
                cache_key,
                fingerprint,
                response_url,
                body_text,
                now,
                now + effective_ttl,
                now,
            ),
        )
        connection.commit()
    finally:
        connection.close()


LIVE_ITEM_LOOKUP_SOURCE_KEYS = {
    "retailer_search_pages",
    "amazon_api_live_offers",
    "pricesapi_live_offers",
    "brightdata_google_shopping",
}


def item_lookup_cache_payload(plan: Dict[str, Any], search_terms: List[str]) -> Dict[str, Any]:
    return {
        "query_type": str(plan.get("query_type") or ""),
        "search_terms": search_terms,
        "requested_pack_count": plan.get("requested_pack_count"),
        "retailers": [str(item) for item in plan.get("retailers", []) if str(item).strip()],
        "official_only": bool(plan.get("official_only")),
    }


def load_item_lookup_cache_state(plan: Dict[str, Any], search_terms: List[str]) -> Tuple[List[Dict[str, Any]], str]:
    if not cache_enabled():
        return [], ""
    payload = item_lookup_cache_payload(plan, search_terms)
    cache_key, _ = stable_cache_key("item_lookup_result", payload)
    now = int(time.time())
    stale_source_key = ""
    connection = open_cache_db()
    try:
        row = connection.execute(
            """
            SELECT namespace, cache_key, request_fingerprint, response_url, body_text, fetched_at, expires_at
            FROM request_cache
            WHERE namespace = ? AND cache_key = ?
            """,
            ("item_lookup_result", cache_key),
        ).fetchone()
        if row is not None and int(row["expires_at"] or 0) > now:
            connection.execute(
                """
                UPDATE request_cache
                SET last_accessed_at = ?, hit_count = hit_count + 1
                WHERE namespace = ? AND cache_key = ?
                """,
                (now, "item_lookup_result", cache_key),
            )
            purge_expired_cache_rows(connection, now, skip_entry=("item_lookup_result", cache_key))
            connection.commit()
            return parse_cached_item_lookup_offers(str(row["body_text"] or ""), mark_cache_hit=True), ""
        if row is not None:
            stale_source_key = archive_item_lookup_cache_row(connection, row, now)
            connection.execute(
                "DELETE FROM request_cache WHERE namespace = ? AND cache_key = ?",
                ("item_lookup_result", cache_key),
            )
        purge_expired_cache_rows(connection, now)
        connection.commit()
    finally:
        connection.close()
    return [], stale_source_key


def store_cached_item_lookup(plan: Dict[str, Any], search_terms: List[str], offers: List[Dict[str, Any]]) -> None:
    if not offers:
        return
    source_key = str(offers[0].get("lookup_source_key") or "")
    if source_key not in LIVE_ITEM_LOOKUP_SOURCE_KEYS:
        return
    cache_put(
        "item_lookup_result",
        item_lookup_cache_payload(plan, search_terms),
        json.dumps({"offers": offers}, sort_keys=True),
        str(offers[0].get("lookup_source_url") or ""),
    )


def first_existing_path(text: str) -> Optional[Path]:
    candidate = text.strip().strip('"').strip("'")
    if not candidate:
        return None
    path = Path(candidate).expanduser()
    return path if path.exists() else None


def extract_urls(text: str) -> List[str]:
    return URL_RE.findall(text or "")


def looks_like_html(text: str) -> bool:
    lowered = (text or "").lower()
    return any(token in lowered for token in ("<html", "<body", "<script", "<div", "<meta"))


def should_use_product_extraction(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False
    path = first_existing_path(stripped)
    if path and path.is_file():
        return True
    if looks_like_html(stripped):
        return True
    urls = extract_urls(stripped)
    if not urls:
        return False
    if len(urls) > 1:
        return True
    remaining = set(WORD_RE.findall(URL_RE.sub(" ", stripped.lower())))
    extraction_hints = {"check", "extract", "html", "link", "page", "price", "product", "url"}
    dataset_hints = {"basket", "dataset", "defra", "food", "foundation", "geolytix", "government", "inflation", "ons", "source", "sources", "statistics", "trend", "trends"}
    if remaining & extraction_hints:
        return True
    if remaining & dataset_hints:
        return False
    return len(remaining) <= 12


def ensure_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError("only http and https URLs are supported")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise RuntimeError("URL is missing a host")
    if host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".local") or host.endswith(".internal"):
        raise RuntimeError("local or private network URLs are not allowed")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        if "." not in host:
            raise RuntimeError("URL must point to a public internet host")
        return
    if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
        raise RuntimeError("local or private network URLs are not allowed")


def fetch_url(url: str) -> Tuple[str, str]:
    ensure_public_url(url)
    cache_payload = {"url": url}
    cached = cache_get("html_url", cache_payload)
    if cached is not None:
        body_text, response_url = cached
        return body_text, response_url or url
    response = requests.get(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    content_type = response.headers.get("content-type", "")
    if "html" not in content_type.lower() and "xml" not in content_type.lower():
        raise RuntimeError(f"expected HTML but got content-type {content_type or 'unknown'}")
    cache_put("html_url", cache_payload, response.text, response.url)
    return response.text, response.url


def fetch_json_url(url: str) -> Tuple[Any, str]:
    ensure_public_url(url)
    cache_payload = {"url": url}
    cached = cache_get("json_url", cache_payload)
    if cached is not None:
        body_text, response_url = cached
        try:
            return json.loads(body_text), response_url or url
        except ValueError:
            pass
    response = requests.get(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
        },
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    cache_put("json_url", cache_payload, json.dumps(payload, sort_keys=True), response.url)
    return payload, response.url


def resolve_source(raw_text: str) -> SourcePayload:
    text = (raw_text or "").strip()
    if not text:
        raise RuntimeError("send a UK food price question, a public product page URL, or pasted HTML")

    path = first_existing_path(text)
    if path and path.is_file():
        if not local_files_allowed():
            raise RuntimeError("local file paths are disabled for this bot surface")
        return SourcePayload(
            kind="file",
            source_label=str(path),
            content=path.read_text(encoding="utf-8", errors="ignore"),
            canonical_url="",
        )

    urls = extract_urls(text)
    if urls:
        html_text, final_url = fetch_url(urls[0])
        return SourcePayload(
            kind="url",
            source_label=urls[0],
            content=html_text,
            canonical_url=final_url,
        )

    if looks_like_html(text):
        return SourcePayload(kind="html", source_label="inline HTML", content=text, canonical_url="")

    raise RuntimeError("send a public product page URL or pasted HTML")


def strip_tags(text: str) -> str:
    return WHITESPACE_RE.sub(" ", html.unescape(TAG_RE.sub(" ", text or ""))).strip()


def first_class_block_text(html_text: str, class_name: str) -> str:
    pattern = re.compile(CLASS_BLOCK_RE_TEMPLATE.format(class_name=re.escape(class_name)), re.IGNORECASE | re.DOTALL)
    match = pattern.search(html_text or "")
    if not match:
        return ""
    return strip_tags(match.group("body"))


def normalize_currency(value: str) -> str:
    raw = (value or "").strip().upper()
    if raw in {"GBP", "USD", "EUR"}:
        return raw
    if "£" in value:
        return "GBP"
    if "$" in value:
        return "USD"
    if "€" in value:
        return "EUR"
    return raw


def currency_symbol(currency: str) -> str:
    return {"GBP": "£", "USD": "$", "EUR": "€"}.get((currency or "").upper(), "")


def parse_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        amount = float(value)
        return amount if math.isfinite(amount) else None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?[0-9]+(?:\.[0-9]+)?", text)
    if not match:
        return None
    try:
        amount = float(match.group(0))
    except ValueError:
        return None
    return amount if math.isfinite(amount) else None


def format_amount(currency: str, amount: Optional[float]) -> str:
    if amount is None:
        return "unknown"
    prefix = currency_symbol(currency)
    formatted = f"{amount:,.2f}"
    return f"{prefix}{formatted}" if prefix else f"{formatted} {currency}".strip()


def format_range(currency: str, low: Optional[float], high: Optional[float]) -> str:
    if low is None and high is None:
        return "unknown"
    if low is None:
        return format_amount(currency, high)
    if high is None or abs(low - high) < 1e-9:
        return format_amount(currency, low)
    return f"{format_amount(currency, low)} to {format_amount(currency, high)}"


def normalize_measure_unit(amount: float, unit: str) -> Tuple[Optional[float], str]:
    unit_key = normalize_text(unit).replace(" ", "")
    if unit_key == "kg":
        return amount, "kg"
    if unit_key == "g":
        return amount / 1000.0, "kg"
    if unit_key == "oz":
        return amount * 0.028349523125, "kg"
    if unit_key == "lb":
        return amount * 0.45359237, "kg"
    if unit_key in {"l", "litre", "liter"}:
        return amount, "l"
    if unit_key == "ml":
        return amount / 1000.0, "l"
    if unit_key == "cl":
        return amount / 100.0, "l"
    if unit_key in {"each", "ea", "unit"}:
        return amount, "each"
    return None, ""


def infer_standard_quantity(product_name: str) -> Tuple[Optional[float], str]:
    text = (product_name or "").strip()
    if not text:
        return None, ""
    for match in MULTIPACK_MEASURE_RE.finditer(text):
        count = parse_amount(match.group(1))
        amount = parse_amount(match.group(2))
        if count is None or amount is None:
            continue
        normalized_amount, normalized_unit = normalize_measure_unit(float(count) * float(amount), match.group(3))
        if normalized_amount is not None and normalized_amount > 0:
            return normalized_amount, normalized_unit
    last_single: Tuple[Optional[float], str] = (None, "")
    for match in SINGLE_MEASURE_RE.finditer(text):
        amount = parse_amount(match.group(1))
        if amount is None:
            continue
        normalized_amount, normalized_unit = normalize_measure_unit(amount, match.group(2))
        if normalized_amount is not None and normalized_amount > 0:
            last_single = (normalized_amount, normalized_unit)
    if last_single[0] is not None:
        return last_single
    count_match = COUNT_PACK_RE.search(text)
    if count_match:
        count = parse_amount(count_match.group(1))
        if count is not None and count > 0:
            return count, "each"
    tail_match = COUNT_TAIL_RE.search(text)
    if tail_match:
        count = parse_amount(tail_match.group(1))
        if count is not None and count > 0:
            return count, "each"
    return None, ""


def extract_requested_pack_count(text: str) -> Optional[int]:
    lowered = (text or "").lower()
    if not lowered:
        return None
    if re.search(r"\bdozen\b", lowered):
        return 12
    patterns = (
        r"\b(?:box|boxes|pack|packs|carton|cartons|tray|trays|case|cases)\s+of\s+(\d+)\b",
        r"\b(\d+)\s*(?:eggs?|pack|packs|box|boxes|carton|cartons|tray|trays|case|cases|pcs?|pieces?)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if not match:
            continue
        count = parse_amount(match.group(1))
        if count is not None and count > 0:
            return int(count)
    return None


def infer_offer_pack_count(product_name: str, unit: str = "") -> Optional[int]:
    lowered_name = (product_name or "").lower()
    lowered_unit = (unit or "").lower()
    if lowered_unit == "dozen":
        return 12
    multipack_match = MULTIPACK_MEASURE_RE.search(lowered_name)
    if multipack_match:
        count = parse_amount(multipack_match.group(1))
        if count is not None and count > 0:
            return int(count)
    dozen_match = re.search(r"\b(\d+)?\s*dozen\b", lowered_name)
    if dozen_match:
        count = parse_amount(dozen_match.group(1) or "1")
        if count is not None and count > 0:
            return int(count * 12)
    count_match = COUNT_PACK_RE.search(lowered_name)
    if count_match:
        count = parse_amount(count_match.group(1))
        if count is not None and count > 0:
            return int(count)
    tail_match = COUNT_TAIL_RE.search(lowered_name)
    if tail_match:
        count = parse_amount(tail_match.group(1))
        if count is not None and count > 0:
            return int(count)
    return None


def pack_count_adjustment(requested_count: Optional[int], offered_count: Optional[int]) -> int:
    if requested_count is None or requested_count <= 0 or offered_count is None or offered_count <= 0:
        return 0
    if offered_count == requested_count:
        return 3
    difference = abs(offered_count - requested_count)
    if difference <= 1:
        return 2
    if difference <= 3:
        return 1
    if offered_count % requested_count == 0 and offered_count <= requested_count * 2:
        return 1
    return -3


def derive_standard_unit_price(price_gbp: Optional[float], product_name: str) -> Tuple[Optional[float], str]:
    if price_gbp is None:
        return None, ""
    quantity, unit = infer_standard_quantity(product_name)
    if quantity is None or quantity <= 0 or not unit:
        return None, ""
    return round(float(price_gbp) / quantity, 2), unit


def extract_standard_unit_price_from_text(text: str) -> Tuple[Optional[float], str]:
    for match in PRICE_PER_STANDARD_UNIT_RE.finditer(text or ""):
        amount = parse_amount(match.group(1))
        denominator = parse_amount(match.group(2) or "1")
        if amount is None or denominator is None or denominator <= 0:
            continue
        normalized_quantity, normalized_unit = normalize_measure_unit(denominator, match.group(3))
        if normalized_quantity is None or normalized_quantity <= 0 or not normalized_unit:
            continue
        return round(amount / normalized_quantity, 2), normalized_unit
    return None, ""


def walk_nodes(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from walk_nodes(nested)
    elif isinstance(value, list):
        for item in value:
            yield from walk_nodes(item)


def coerce_graph_nodes(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, dict) and "@graph" in payload:
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                if isinstance(item, dict):
                    yield item
        return
    if isinstance(payload, dict):
        yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item


def extract_jsonld_objects(html_text: str) -> List[Any]:
    objects: List[Any] = []
    for block in SCRIPT_JSONLD_RE.findall(html_text):
        cleaned = html.unescape(block).strip()
        if not cleaned:
            continue
        try:
            objects.append(json.loads(cleaned))
            continue
        except Exception:
            pass
        fixed = re.sub(r"\s+", " ", cleaned)
        try:
            objects.append(json.loads(fixed))
        except Exception:
            continue
    return objects


def extract_from_jsonld(html_text: str) -> Dict[str, Any]:
    best: Dict[str, Any] = {}
    best_score = -1
    for obj in extract_jsonld_objects(html_text):
        for root in coerce_graph_nodes(obj):
            for node in walk_nodes(root):
                node_type = node.get("@type")
                if isinstance(node_type, str):
                    types = [node_type]
                elif isinstance(node_type, list):
                    types = [str(item) for item in node_type]
                else:
                    types = []
                if not any(item in {"Product", "Offer", "AggregateOffer"} for item in types):
                    continue
                candidate: Dict[str, Any] = {}
                if "Product" in types:
                    candidate["product_name"] = strip_tags(str(node.get("name", "")))
                    candidate["canonical_url"] = str(node.get("url", "")).strip()
                    offers = node.get("offers")
                    if isinstance(offers, list):
                        offer_list = [item for item in offers if isinstance(item, dict)]
                    elif isinstance(offers, dict):
                        offer_list = [offers]
                    else:
                        offer_list = []
                    for offer in offer_list:
                        offer_type = str(offer.get("@type", ""))
                        if offer_type == "AggregateOffer":
                            candidate["low_price"] = parse_amount(offer.get("lowPrice"))
                            candidate["high_price"] = parse_amount(offer.get("highPrice"))
                        else:
                            price = parse_amount(offer.get("price"))
                            if price is not None:
                                candidate["current_price"] = price
                        currency = normalize_currency(str(offer.get("priceCurrency", "")))
                        if currency:
                            candidate["currency"] = currency
                        availability = strip_tags(str(offer.get("availability", "")))
                        if availability:
                            candidate["availability"] = availability.rsplit("/", 1)[-1]
                if "Offer" in types:
                    price = parse_amount(node.get("price"))
                    if price is not None:
                        candidate.setdefault("current_price", price)
                    currency = normalize_currency(str(node.get("priceCurrency", "")))
                    if currency:
                        candidate.setdefault("currency", currency)
                if "AggregateOffer" in types:
                    low = parse_amount(node.get("lowPrice"))
                    high = parse_amount(node.get("highPrice"))
                    if low is not None:
                        candidate.setdefault("low_price", low)
                    if high is not None:
                        candidate.setdefault("high_price", high)
                    currency = normalize_currency(str(node.get("priceCurrency", "")))
                    if currency:
                        candidate.setdefault("currency", currency)
                score = sum(1 for key in ("product_name", "current_price", "low_price", "high_price", "currency") if candidate.get(key) not in {None, ""})
                if score > best_score:
                    best = candidate
                    best_score = score
    return best


def extract_from_variations(html_text: str) -> Dict[str, Any]:
    current_prices: List[float] = []
    regular_prices: List[float] = []
    for _, raw in DATA_VARIATIONS_RE.findall(html_text):
        decoded = html.unescape(raw)
        try:
            data = json.loads(decoded)
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            price = parse_amount(item.get("display_price"))
            regular = parse_amount(item.get("display_regular_price"))
            if price is not None:
                current_prices.append(price)
            if regular is not None:
                regular_prices.append(regular)
    result: Dict[str, Any] = {}
    if current_prices:
        result["low_price"] = min(current_prices)
        result["high_price"] = max(current_prices)
        if len(set(current_prices)) == 1:
            result["current_price"] = current_prices[0]
    if regular_prices:
        result["regular_low_price"] = min(regular_prices)
        result["regular_high_price"] = max(regular_prices)
        if len(set(regular_prices)) == 1:
            result["regular_price"] = regular_prices[0]
    return result


def extract_from_meta(html_text: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    price_match = META_PRICE_RE.search(html_text)
    if price_match:
        amount = parse_amount(price_match.group(1))
        if amount is not None:
            result["current_price"] = amount
    currency_match = META_CURRENCY_RE.search(html_text)
    if currency_match:
        result["currency"] = normalize_currency(currency_match.group(1))
    return result


def parse_retail_price_text(text: str) -> Optional[float]:
    cleaned = strip_tags(text)
    if not cleaned:
        return None
    leading = cleaned.split("/", 1)[0].strip()
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?\s*p", leading, re.IGNORECASE):
        amount = parse_amount(leading)
        return None if amount is None else round(amount / 100.0, 2)
    return parse_amount(leading)


def extract_sainsburys_html_price(html_text: str) -> Dict[str, Any]:
    block_text = first_class_block_text(html_text, "pricePerUnit")
    if not block_text:
        return {}
    result: Dict[str, Any] = {"currency": "GBP"}
    current_price = parse_retail_price_text(block_text)
    if current_price is not None:
        result["current_price"] = current_price
    unit_price, unit = extract_standard_unit_price_from_text(block_text)
    if unit_price is not None and unit:
        result["price_unit_gbp"] = unit_price
        result["unit"] = unit
    return result


def extract_morrisons_html_price(html_text: str) -> Dict[str, Any]:
    for class_name in ("nowPrice", "typicalPrice"):
        block_text = first_class_block_text(html_text, class_name)
        current_price = parse_retail_price_text(block_text)
        if current_price is not None:
            return {"current_price": current_price, "currency": "GBP"}
    return {}


def extract_tesco_html_price(html_text: str) -> Dict[str, Any]:
    block_text = first_class_block_text(html_text, "value")
    current_price = parse_retail_price_text(block_text)
    if current_price is None:
        return {}
    return {"current_price": current_price, "currency": "GBP"}


def extract_from_known_retailer_html(html_text: str, url: str) -> Dict[str, Any]:
    retailer = retailer_name_from_url(url)
    if retailer == "Sainsbury's":
        return extract_sainsburys_html_price(html_text)
    if retailer == "Morrisons":
        return extract_morrisons_html_price(html_text)
    if retailer == "Tesco":
        return extract_tesco_html_price(html_text)
    return {}


def extract_from_title(html_text: str) -> Dict[str, Any]:
    match = TITLE_RE.search(html_text)
    if not match:
        return {}
    title = strip_tags(match.group(1))
    return {"product_name": title} if title else {}


def extract_amounts_from_text(html_text: str) -> Dict[str, Any]:
    amounts = [parse_amount(match.group(1)) for match in AMOUNT_RE.finditer(html_text)]
    clean = [value for value in amounts if value is not None]
    if not clean:
        return {}
    result: Dict[str, Any] = {"low_price": min(clean), "high_price": max(clean)}
    if len(set(clean)) == 1:
        result["current_price"] = clean[0]
    return result


def extract_availability(html_text: str) -> str:
    text = strip_tags(html_text)
    match = TEXT_AVAILABILITY_RE.search(text)
    if not match:
        return ""
    value = match.group(1).lower()
    return {
        "in stock": "In stock",
        "out of stock": "Out of stock",
        "available": "Available",
        "unavailable": "Unavailable",
    }.get(value, match.group(1))


def merge_values(*parts: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for part in parts:
        for key, value in part.items():
            if value in (None, "", [], {}):
                continue
            if key not in merged or merged[key] in (None, "", [], {}):
                merged[key] = value
    return merged


def compute_discount(result: Dict[str, Any]) -> Optional[float]:
    current = result.get("current_price")
    regular = result.get("regular_price")
    if current is None and result.get("low_price") is not None:
        current = result.get("low_price")
    if regular is None and result.get("regular_low_price") is not None:
        regular = result.get("regular_low_price")
    if current is None or regular is None or regular <= 0 or regular <= current:
        return None
    return round(((regular - current) / regular) * 100.0, 1)


def error_result(source: Optional[SourcePayload], message: str) -> PriceResult:
    source_label = source.source_label if source else ""
    reply = f"Error: {message}\nSend a UK food price question, a public product page URL, or pasted HTML."
    return PriceResult(
        ok=False,
        summary="Price lookup failed",
        source=source_label,
        error_message=message,
        reply_message=reply,
    )


def build_product_result(source: SourcePayload, merged: Dict[str, Any]) -> PriceResult:
    currency = normalize_currency(str(merged.get("currency", "")))
    product_name = str(merged.get("product_name", "")).strip()
    canonical_url = str(merged.get("canonical_url") or source.canonical_url or source.source_label).strip()
    availability = str(merged.get("availability", "")).strip()
    if availability.startswith("http"):
        availability = availability.rsplit("/", 1)[-1]
    if not availability:
        availability = extract_availability(source.content)

    current_price = parse_amount(merged.get("current_price"))
    regular_price = parse_amount(merged.get("regular_price"))
    low_price = parse_amount(merged.get("low_price"))
    high_price = parse_amount(merged.get("high_price"))
    regular_low = parse_amount(merged.get("regular_low_price"))
    regular_high = parse_amount(merged.get("regular_high_price"))

    if current_price is None and low_price is not None and high_price is not None and abs(low_price - high_price) < 1e-9:
        current_price = low_price
    if regular_price is None and regular_low is not None and regular_high is not None and abs(regular_low - regular_high) < 1e-9:
        regular_price = regular_low

    if current_price is None and low_price is None and high_price is None:
        return error_result(source, "could not extract a product price from the supplied input")

    if not product_name:
        parsed = urlparse(canonical_url)
        if parsed.path and parsed.path != "/":
            product_name = parsed.path.rstrip("/").split("/")[-1].replace("-", " ").strip().title()
        else:
            product_name = "Product"

    discount = compute_discount(
        {
            "current_price": current_price,
            "regular_price": regular_price,
            "low_price": low_price,
            "regular_low_price": regular_low,
        }
    )

    if low_price is not None and high_price is not None and abs(low_price - high_price) >= 1e-9:
        current_text = format_range(currency, low_price, high_price)
    else:
        current_text = format_amount(currency, current_price if current_price is not None else low_price)

    if regular_price is not None:
        regular_text = format_amount(currency, regular_price)
    elif regular_low is not None or regular_high is not None:
        regular_text = format_range(currency, regular_low, regular_high)
    else:
        regular_text = "not found"

    availability_text = availability or "not stated"
    discount_text = f"{discount:.1f}% off" if discount is not None else "not found"
    source_text = canonical_url or source.source_label
    summary = f"{product_name}: {current_text}"
    reply_message = (
        f"Product: {product_name}\n"
        f"Current price: {current_text}\n"
        f"Regular price: {regular_text}\n"
        f"Discount: {discount_text}\n"
        f"Availability: {availability_text}\n"
        f"Source: {source_text}"
    )

    return PriceResult(
        ok=True,
        summary=summary,
        source=source.source_label,
        product_name=product_name,
        canonical_url=canonical_url,
        currency=currency,
        current_price=current_price,
        regular_price=regular_price,
        low_price=low_price,
        high_price=high_price,
        regular_low_price=regular_low,
        regular_high_price=regular_high,
        availability=availability_text,
        discount_percent=discount,
        error_message="",
        reply_message=reply_message,
        mode="product",
    )


def ordered_unique(items: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", re.sub(r"[^a-z0-9]+", " ", (value or "").lower())).strip()


def normalize_retailer_key(value: str) -> str:
    return normalize_text(value).replace(" ", "")


def retailer_display_name(value: str) -> str:
    normalized = normalize_retailer_key(value)
    return CANONICAL_RETAILER_NAMES.get(normalized, DATASET_RETAILER_NAMES.get(normalized, (value or "").strip() or "Unknown retailer"))


def retailer_name_from_url(url: str) -> str:
    host = (urlparse(url).hostname or "").strip().lower()
    if not host:
        return ""
    host = host[4:] if host.startswith("www.") else host
    for suffix, retailer in RETAILER_HOST_NAMES.items():
        if host == suffix or host.endswith(f".{suffix}"):
            return retailer
    return ""


def offer_metric(offer: Dict[str, Any]) -> float:
    for key in ("price_unit_gbp", "price_gbp"):
        value = offer.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            return float(value)
    return float("inf")


def capture_rank(value: str) -> int:
    digits = re.sub(r"[^0-9]", "", (value or "")[:10])
    return int(digits) if digits else 0


def extract_item_terms(plan: Dict[str, Any]) -> List[str]:
    return [term for term in plan.get("focus_terms", []) if len(term) > 2][:3]


def should_try_direct_item_lookup(plan: Dict[str, Any]) -> bool:
    if plan.get("official_only"):
        return False
    if plan.get("query_type") not in {"retailer_comparison", "item_price_lookup"}:
        return False
    terms = extract_item_terms(plan)
    if not terms:
        return False
    if plan.get("retailers"):
        return True
    if len(terms) >= 2:
        return True
    if plan.get("requested_pack_count") is not None:
        return True
    return terms[0] not in BROAD_ITEM_TERMS




def requested_retailer_keys(plan: Dict[str, Any]) -> set[str]:
    return {
        normalize_retailer_key(name)
        for name in plan.get("retailers", [])
        if str(name).strip()
    }


def retailer_requested(plan: Dict[str, Any], retailer_name: str) -> bool:
    requested = requested_retailer_keys(plan)
    if not requested:
        return True
    return normalize_retailer_key(retailer_name) in requested


def matching_term_score(
    normalized_name: str,
    search_terms: List[str],
    *,
    product_name: str = "",
    requested_pack_count: Optional[int] = None,
    unit: str = "",
) -> int:
    if not normalized_name:
        return 0
    primary_term = search_terms[0]
    if primary_term not in normalized_name:
        return 0
    score = sum(1 for term in search_terms if term in normalized_name)
    phrase = " ".join(search_terms)
    if len(search_terms) > 1 and phrase and phrase in normalized_name:
        score += 1
    if len(search_terms) == 1 and search_terms[0] == "eggs":
        if any(term in normalized_name for term in EGG_QUERY_EXCLUDED_TERMS):
            return 0
    score += pack_count_adjustment(requested_pack_count, infer_offer_pack_count(product_name or normalized_name, unit))
    return score


def better_retailer_offer(existing: Optional[Dict[str, Any]], candidate: Dict[str, Any]) -> Dict[str, Any]:
    if existing is None:
        return candidate
    if candidate.get("score", 0) > existing.get("score", 0):
        return candidate
    if candidate.get("score", 0) < existing.get("score", 0):
        return existing
    if offer_metric(candidate) < offer_metric(existing):
        return candidate
    if offer_metric(candidate) > offer_metric(existing):
        return existing
    return candidate if capture_rank(candidate.get("capture_date", "")) > capture_rank(existing.get("capture_date", "")) else existing


def collect_offer_candidate(row: Dict[str, Any], search_terms: List[str], requested_pack_count: Optional[int] = None) -> Optional[Dict[str, Any]]:
    product_name = str(row.get("product_name") or "").strip()
    normalized_name = str(row.get("normalized_product_name") or normalize_text(product_name)).strip()
    category_name = str(row.get("category_name") or "").strip().lower()
    if category_name and category_name not in FOOD_CATEGORY_ALLOWLIST:
        return None
    unit = str(row.get("unit") or "").strip()
    score = matching_term_score(
        normalized_name,
        search_terms,
        product_name=product_name,
        requested_pack_count=requested_pack_count,
        unit=unit,
    )
    if score <= 0:
        return None
    price_gbp = parse_amount(row.get("price_gbp"))
    price_unit_gbp = parse_amount(row.get("price_unit_gbp"))
    if price_gbp is None and price_unit_gbp is None:
        return None
    return {
        "retailer": retailer_display_name(str(row.get("supermarket_name") or "")),
        "product_name": product_name,
        "price_gbp": price_gbp,
        "price_unit_gbp": price_unit_gbp,
        "unit": unit,
        "capture_date": str(row.get("capture_date") or "").strip(),
        "category_name": category_name,
        "is_own_brand": str(row.get("is_own_brand") or "").strip().lower() in {"1", "true", "yes"},
        "score": score,
    }


def annotate_lookup_source(
    offers: List[Dict[str, Any]],
    *,
    source_key: str,
    source_name: str,
    source_url: str,
) -> List[Dict[str, Any]]:
    annotated: List[Dict[str, Any]] = []
    for offer in offers:
        enriched = dict(offer)
        enriched.setdefault("lookup_source_key", source_key)
        enriched.setdefault("lookup_source_name", source_name)
        enriched.setdefault("lookup_source_url", source_url)
        annotated.append(enriched)
    return annotated


def brightdata_api_key() -> str:
    return os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY", "").strip()


def brightdata_zone() -> str:
    return os.getenv("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE", "").strip()


def brightdata_enabled() -> bool:
    return bool(brightdata_api_key() and brightdata_zone())


def pricesapi_api_key() -> str:
    return os.getenv("SPRINGFIELD_PRICE_PRICESAPI_KEY", "").strip()


def pricesapi_enabled() -> bool:
    return bool(pricesapi_api_key())


def fetch_pricesapi_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    cache_payload = {"base_url": PRICESAPI_BASE_URL, "path": path, "params": params}
    cached = cache_get("pricesapi_json", cache_payload)
    if cached is not None:
        body_text, _ = cached
        try:
            payload = json.loads(body_text)
        except ValueError:
            payload = {}
        return payload if isinstance(payload, dict) else {}
    response = requests.get(
        f"{PRICESAPI_BASE_URL}{path}",
        headers={
            "x-api-key": pricesapi_api_key(),
            "User-Agent": DEFAULT_USER_AGENT,
        },
        params=params,
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    cache_put("pricesapi_json", cache_payload, json.dumps(payload, sort_keys=True), f"{PRICESAPI_BASE_URL}{path}")
    if not isinstance(payload, dict):
        return {}
    return payload


def fetch_pricesapi_search_results(query: str) -> List[Dict[str, Any]]:
    if not pricesapi_enabled():
        return []
    payload = fetch_pricesapi_json(
        "/products/search",
        {"q": query, "limit": PRICESAPI_SEARCH_LIMIT},
    )
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    results = data.get("results")
    if not isinstance(results, list):
        return []
    return [item for item in results if isinstance(item, dict)]


def fetch_pricesapi_product_offers(product_id: Any) -> List[Dict[str, Any]]:
    if not pricesapi_enabled():
        return []
    payload = fetch_pricesapi_json(
        f"/products/{product_id}/offers",
        {"country": PRICESAPI_COUNTRY},
    )
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    offers = data.get("offers")
    if not isinstance(offers, list):
        return []
    return [item for item in offers if isinstance(item, dict)]


def score_pricesapi_product(result: Dict[str, Any], search_terms: List[str], requested_pack_count: Optional[int] = None) -> int:
    title = str(result.get("title") or "").strip()
    if not title:
        return 0
    score = matching_term_score(
        normalize_text(title),
        search_terms,
        product_name=title,
        requested_pack_count=requested_pack_count,
    )
    offer_count = int(parse_amount(result.get("offerCount")) or 0)
    if offer_count > 0:
        score += 1
    return score


def amazon_paapi_access_key() -> str:
    return os.getenv("SPRINGFIELD_PRICE_AMAZON_API_ACCESS_KEY", "").strip()


def amazon_paapi_secret_key() -> str:
    return os.getenv("SPRINGFIELD_PRICE_AMAZON_API_SECRET_KEY", "").strip()


def amazon_paapi_partner_tag() -> str:
    return os.getenv("SPRINGFIELD_PRICE_AMAZON_API_PARTNER_TAG", "").strip()


def amazon_paapi_enabled() -> bool:
    return bool(amazon_paapi_access_key() and amazon_paapi_secret_key() and amazon_paapi_partner_tag())


def aws_sign_hmac(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()


def build_amazon_paapi_headers(path: str, target: str, payload_text: str) -> Dict[str, str]:
    access_key = amazon_paapi_access_key()
    secret_key = amazon_paapi_secret_key()
    now = time.gmtime()
    amz_date = time.strftime("%Y%m%dT%H%M%SZ", now)
    date_stamp = time.strftime("%Y%m%d", now)
    service = "ProductAdvertisingAPI"
    canonical_headers = (
        "content-encoding:amz-1.0\n"
        "content-type:application/json; charset=utf-8\n"
        f"host:{AMAZON_PAAPI_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:{target}\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
    canonical_request = "\n".join(
        [
            "POST",
            path,
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{AMAZON_PAAPI_REGION}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    k_date = aws_sign_hmac(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = aws_sign_hmac(k_date, AMAZON_PAAPI_REGION)
    k_service = aws_sign_hmac(k_region, service)
    k_signing = aws_sign_hmac(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return {
        "Content-Encoding": "amz-1.0",
        "Content-Type": "application/json; charset=utf-8",
        "Host": AMAZON_PAAPI_HOST,
        "X-Amz-Date": amz_date,
        "X-Amz-Target": target,
        "Authorization": authorization,
        "User-Agent": DEFAULT_USER_AGENT,
    }


def fetch_amazon_paapi_json(path: str, target: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not amazon_paapi_enabled():
        return {}
    cache_payload = {"host": AMAZON_PAAPI_HOST, "path": path, "target": target, "payload": payload}
    cached = cache_get("amazon_paapi_json", cache_payload)
    if cached is not None:
        body_text, _ = cached
        try:
            decoded = json.loads(body_text)
        except ValueError:
            decoded = {}
        return decoded if isinstance(decoded, dict) else {}
    payload_text = json.dumps(payload, sort_keys=True)
    headers = build_amazon_paapi_headers(path, target, payload_text)
    endpoint = f"https://{AMAZON_PAAPI_HOST}{path}"
    response = requests.post(
        endpoint,
        headers=headers,
        data=payload_text,
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    decoded = response.json()
    cache_put("amazon_paapi_json", cache_payload, json.dumps(decoded, sort_keys=True), endpoint)
    return decoded if isinstance(decoded, dict) else {}


def fetch_amazon_paapi_search_items(query: str) -> List[Dict[str, Any]]:
    if not amazon_paapi_enabled():
        return []
    payload = {
        "Keywords": query,
        "ItemCount": AMAZON_PAAPI_ITEM_LIMIT,
        "SearchIndex": AMAZON_PAAPI_SEARCH_INDEX,
        "Marketplace": AMAZON_PAAPI_MARKETPLACE,
        "PartnerTag": amazon_paapi_partner_tag(),
        "PartnerType": AMAZON_PAAPI_PARTNER_TYPE,
        "Resources": [
            "ItemInfo.Title",
            "Offers.Listings.Price",
            "Offers.Summaries.LowestPrice",
        ],
    }
    response_payload = fetch_amazon_paapi_json(
        "/paapi5/searchitems",
        "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.SearchItems",
        payload,
    )
    search_result = response_payload.get("SearchResult")
    if not isinstance(search_result, dict):
        return []
    items = search_result.get("Items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def extract_amazon_paapi_price(item: Dict[str, Any]) -> Optional[float]:
    offers = item.get("Offers")
    if isinstance(offers, dict):
        listings = offers.get("Listings")
        if isinstance(listings, list):
            for listing in listings:
                if not isinstance(listing, dict):
                    continue
                price = listing.get("Price")
                if not isinstance(price, dict):
                    continue
                currency = normalize_currency(str(price.get("Currency") or ""))
                if currency and currency != "GBP":
                    continue
                amount = parse_amount(price.get("Amount"))
                if amount is None:
                    amount = parse_amount(price.get("DisplayAmount"))
                if amount is not None:
                    return amount
        summaries = offers.get("Summaries")
        if isinstance(summaries, list):
            for summary in summaries:
                if not isinstance(summary, dict):
                    continue
                lowest = summary.get("LowestPrice")
                if not isinstance(lowest, dict):
                    continue
                currency = normalize_currency(str(lowest.get("Currency") or ""))
                if currency and currency != "GBP":
                    continue
                amount = parse_amount(lowest.get("Amount"))
                if amount is not None:
                    return amount
    return None


def collect_amazon_paapi_offer(
    item: Dict[str, Any],
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    detail_url = str(item.get("DetailPageURL") or "").strip()
    item_info = item.get("ItemInfo")
    title_info = item_info.get("Title") if isinstance(item_info, dict) else None
    product_name = str(title_info.get("DisplayValue") if isinstance(title_info, dict) else "").strip()
    if not product_name:
        return None
    price_gbp = extract_amazon_paapi_price(item)
    return collect_live_offer(
        "Amazon UK",
        product_name,
        price_gbp,
        detail_url,
        search_terms,
        requested_pack_count=requested_pack_count,
    )


def build_brightdata_google_shopping_url(query: str) -> str:
    encoded_query = quote_plus(query)
    return f"https://{BRIGHTDATA_SERP_HOST}/search?q={encoded_query}&tbm=shop&gl={BRIGHTDATA_SERP_GEO}&hl={BRIGHTDATA_SERP_LANGUAGE}"


def fetch_brightdata_shopping_results(query: str) -> List[Dict[str, Any]]:
    if not brightdata_enabled():
        return []
    payload = {
        "endpoint": BRIGHTDATA_SERP_ENDPOINT,
        "zone": brightdata_zone(),
        "url": build_brightdata_google_shopping_url(query),
        "format": "json",
        "method": "GET",
        "country": BRIGHTDATA_SERP_COUNTRY,
    }
    cached = cache_get("brightdata_shopping", payload)
    if cached is not None:
        body_text, _ = cached
        try:
            raw = json.loads(body_text)
        except ValueError:
            return []
        return extract_brightdata_shopping_results(raw)
    response = requests.post(
        BRIGHTDATA_SERP_ENDPOINT,
        headers={
            "Authorization": f"Bearer {brightdata_api_key()}",
            "Content-Type": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
        },
        json=payload,
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    try:
        raw = response.json()
    except ValueError:
        return []
    cache_put("brightdata_shopping", payload, json.dumps(raw, sort_keys=True), BRIGHTDATA_SERP_ENDPOINT)
    return extract_brightdata_shopping_results(raw)


def extract_brightdata_shopping_results(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        shopping = payload.get("shopping")
        if isinstance(shopping, list):
            return [item for item in shopping if isinstance(item, dict)]
        for key in ("body", "result", "results", "response", "data"):
            nested = payload.get(key)
            results = extract_brightdata_shopping_results(nested)
            if results:
                return results
        return []
    if isinstance(payload, list):
        for item in payload:
            results = extract_brightdata_shopping_results(item)
            if results:
                return results
    return []


def csv_retailer_shortlist(plan: Dict[str, Any], csv_offers: List[Dict[str, Any]]) -> List[str]:
    shortlist = ordered_unique(str(offer.get("retailer") or "").strip() for offer in csv_offers if offer.get("retailer"))
    shortlist = [retailer for retailer in shortlist if retailer_requested(plan, retailer)]
    if shortlist:
        return shortlist[:3]
    return [retailer for retailer in plan.get("retailers", []) if str(retailer).strip()][:3]


def add_lookup_context(
    offers: List[Dict[str, Any]],
    *,
    shortlist: List[str],
) -> List[Dict[str, Any]]:
    matched_keys = {normalize_retailer_key(str(offer.get("retailer") or "")) for offer in offers}
    missing = [retailer for retailer in shortlist if normalize_retailer_key(retailer) not in matched_keys]
    contextualized: List[Dict[str, Any]] = []
    for offer in offers:
        enriched = dict(offer)
        enriched["csv_shortlist_retailers"] = shortlist
        enriched["csv_shortlist_missing_retailers"] = missing
        contextualized.append(enriched)
    return contextualized


def collect_pricesapi_offer(
    offer: Dict[str, Any],
    search_terms: List[str],
    shortlist_keys: set[str],
    requested_pack_count: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    product_name = str(offer.get("productTitle") or offer.get("title") or "").strip()
    if not product_name:
        return None
    product_url = str(offer.get("url") or "").strip()
    seller = str(offer.get("seller") or "").strip()
    retailer = retailer_display_name(seller or retailer_name_from_url(product_url))
    if shortlist_keys and normalize_retailer_key(retailer) not in shortlist_keys:
        return None
    price_gbp = parse_amount(offer.get("price"))
    currency = normalize_currency(str(offer.get("currency") or ""))
    if price_gbp is None or (currency and currency != "GBP"):
        return None
    score = matching_term_score(
        normalize_text(product_name),
        search_terms,
        product_name=product_name,
        requested_pack_count=requested_pack_count,
    )
    if score <= 0:
        return None
    return {
        "retailer": retailer,
        "product_name": product_name,
        "price_gbp": price_gbp,
        "price_unit_gbp": None,
        "unit": "",
        "capture_date": "",
        "score": score,
        "product_url": product_url,
    }


def find_pricesapi_offers(plan: Dict[str, Any], search_terms: List[str], csv_offers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not pricesapi_enabled():
        return []
    shortlist = csv_retailer_shortlist(plan, csv_offers)
    shortlist_keys = {normalize_retailer_key(retailer) for retailer in shortlist}
    query = " ".join(search_terms).strip()
    if not query:
        return []
    try:
        search_results = fetch_pricesapi_search_results(query)
    except Exception:
        return []
    ranked_products = sorted(
        (
            {
                "id": result.get("id"),
                "title": str(result.get("title") or "").strip(),
                "score": score_pricesapi_product(result, search_terms, plan.get("requested_pack_count")),
            }
            for result in search_results
            if result.get("id") is not None
        ),
        key=lambda item: (-int(item.get("score", 0)), str(item.get("title") or "")),
    )
    best_by_retailer: Dict[str, Dict[str, Any]] = {}
    for product in ranked_products[:PRICESAPI_PRODUCT_LIMIT]:
        if int(product.get("score", 0)) <= 0:
            continue
        try:
            offers = fetch_pricesapi_product_offers(product.get("id"))
        except Exception:
            continue
        for offer in offers:
            candidate = collect_pricesapi_offer(offer, search_terms, shortlist_keys, plan.get("requested_pack_count"))
            if candidate is None:
                continue
            retailer = str(candidate.get("retailer") or "Unknown retailer")
            best_by_retailer[retailer] = better_retailer_offer(best_by_retailer.get(retailer), candidate)
        if len(best_by_retailer) >= len(shortlist_keys):
            break
    if not best_by_retailer:
        return []
    ranked = sorted(
        best_by_retailer.values(),
        key=lambda offer: (
            -int(offer.get("score", 0)),
            offer_metric(offer),
            str(offer.get("retailer") or ""),
            str(offer.get("product_name") or ""),
        ),
    )[:3]
    return annotate_lookup_source(
        add_lookup_context(ranked, shortlist=shortlist),
        source_key="pricesapi_live_offers",
        source_name="PricesAPI live offers",
        source_url=PRICESAPI_DOCS_URL,
    )


def find_amazon_api_offers(plan: Dict[str, Any], search_terms: List[str]) -> List[Dict[str, Any]]:
    if not retailer_requested(plan, "Amazon UK"):
        return []
    if not amazon_paapi_enabled():
        return []
    query = " ".join(search_terms).strip()
    if not query:
        return []
    try:
        items = fetch_amazon_paapi_search_items(query)
    except Exception:
        return []
    best_offer: Optional[Dict[str, Any]] = None
    for item in items:
        offer = collect_amazon_paapi_offer(item, search_terms, plan.get("requested_pack_count"))
        if offer is None:
            continue
        best_offer = better_retailer_offer(best_offer, offer)
    if best_offer is None:
        return []
    return annotate_lookup_source(
        [best_offer],
        source_key="amazon_api_live_offers",
        source_name="Amazon Product Advertising API",
        source_url=AMAZON_PAAPI_DOCS_URL,
    )


def collect_brightdata_offer(
    result: Dict[str, Any],
    search_terms: List[str],
    retailer_hint: str,
    requested_pack_count: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    product_name = str(result.get("title") or result.get("name") or "").strip()
    if not product_name:
        return None
    product_url = str(result.get("link") or result.get("url") or result.get("product_url") or "").strip()
    shop_text = str(result.get("shop") or result.get("merchant") or result.get("seller") or result.get("source") or "").strip()
    retailer = retailer_display_name(shop_text or retailer_name_from_url(product_url) or retailer_hint)
    if normalize_retailer_key(retailer) != normalize_retailer_key(retailer_hint):
        return None
    price_gbp = parse_amount(result.get("price"))
    if price_gbp is None:
        price_gbp = parse_amount(result.get("extracted_price"))
    if price_gbp is None:
        return None
    normalized_name = normalize_text(product_name)
    score = matching_term_score(
        normalized_name,
        search_terms,
        product_name=product_name,
        requested_pack_count=requested_pack_count,
    )
    if score <= 0:
        return None
    return {
        "retailer": retailer,
        "product_name": product_name,
        "price_gbp": price_gbp,
        "price_unit_gbp": None,
        "unit": "",
        "capture_date": "",
        "score": score,
        "product_url": product_url,
    }


def find_brightdata_shopping_offers(plan: Dict[str, Any], search_terms: List[str], csv_offers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not brightdata_enabled():
        return []
    shortlist = csv_retailer_shortlist(plan, csv_offers)
    if not shortlist:
        return []
    cache: Dict[str, List[Dict[str, Any]]] = {}
    offers: List[Dict[str, Any]] = []
    for retailer in shortlist:
        query = " ".join(search_terms + [retailer]).strip()
        if not query:
            continue
        try:
            results = cache.setdefault(query, fetch_brightdata_shopping_results(query))
        except Exception:
            continue
        best_offer: Optional[Dict[str, Any]] = None
        for result in results:
            offer = collect_brightdata_offer(result, search_terms, retailer, plan.get("requested_pack_count"))
            if offer is None:
                continue
            best_offer = better_retailer_offer(best_offer, offer)
        if best_offer is not None:
            offers.append(best_offer)
    if not offers:
        return []
    ranked = sorted(
        offers,
        key=lambda offer: (
            -int(offer.get("score", 0)),
            offer_metric(offer),
            str(offer.get("retailer") or ""),
            str(offer.get("product_name") or ""),
        ),
    )[:3]
    return annotate_lookup_source(
        add_lookup_context(ranked, shortlist=shortlist),
        source_key="brightdata_google_shopping",
        source_name="Bright Data Google Shopping",
        source_url=BRIGHTDATA_GOOGLE_SHOPPING_DOCS_URL,
    )


def should_try_live_merchant_lookup(plan: Dict[str, Any], search_terms: List[str]) -> bool:
    if plan.get("official_only"):
        return False
    if plan.get("query_type") not in {"retailer_comparison", "item_price_lookup"}:
        return False
    if not search_terms:
        return False
    if plan.get("retailers"):
        if any(bool(source.get("supports_retailer_lookup")) for source in MERCHANT_SEARCH_SOURCES):
            return True
        merchant_keys = {
            normalize_retailer_key(str(source.get("retailer") or source.get("name") or ""))
            for source in MERCHANT_SEARCH_SOURCES
            if str(source.get("retailer") or source.get("name") or "").strip()
        }
        return bool(requested_retailer_keys(plan) & merchant_keys)
    if plan.get("requested_pack_count") is not None:
        return True
    return len(search_terms) >= 2 or search_terms[0] not in BROAD_ITEM_TERMS


def extract_braced_object(text: str, start_index: int) -> Tuple[str, int]:
    brace_index = text.find("{", start_index)
    if brace_index < 0:
        return "", -1
    depth = 0
    quote = ""
    escaping = False
    for index in range(brace_index, len(text)):
        char = text[index]
        if quote:
            if escaping:
                escaping = False
            elif char == "\\":
                escaping = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'"}:
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[brace_index:index + 1], index + 1
    return "", -1


def collect_live_offer(
    retailer: str,
    product_name: str,
    price_gbp: Optional[float],
    product_url: str,
    search_terms: List[str],
    *,
    price_unit_gbp: Optional[float] = None,
    unit: str = "",
    requested_pack_count: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if price_gbp is None:
        return None
    normalized_name = normalize_text(product_name)
    score = matching_term_score(
        normalized_name,
        search_terms,
        product_name=product_name,
        requested_pack_count=requested_pack_count,
        unit=unit,
    )
    if score <= 0:
        return None
    if any(term in normalized_name for term in MERCHANT_EXCLUDED_TERMS if term not in search_terms):
        return None
    if price_unit_gbp is None:
        price_unit_gbp, derived_unit = derive_standard_unit_price(price_gbp, product_name)
        if not unit and derived_unit:
            unit = derived_unit
    return {
        "retailer": retailer,
        "product_name": product_name,
        "price_gbp": price_gbp,
        "price_unit_gbp": price_unit_gbp,
        "unit": unit,
        "capture_date": "",
        "score": score,
        "product_url": product_url,
    }


def parse_wlfdn_shopify_results(
    html_text: str,
    source: Dict[str, str],
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    offers: List[Dict[str, Any]] = []
    for match in WLFDN_PRODUCT_PUSH_RE.finditer(html_text):
        block = match.group(1)
        handle_match = WLFDN_HANDLE_RE.search(block)
        name_match = WLFDN_NAME_RE.search(block)
        price_match = WLFDN_PRICE_RE.search(block)
        if not handle_match or not name_match or not price_match:
            continue
        offer = collect_live_offer(
            source["name"],
            html.unescape(name_match.group(1)).strip(),
            parse_amount(price_match.group(1)),
            urljoin(source["product_base_url"], handle_match.group(1).strip()),
            search_terms,
            requested_pack_count=requested_pack_count,
        )
        if offer:
            offers.append(offer)
    return offers


def parse_shopify_meta_results(
    html_text: str,
    source: Dict[str, str],
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    marker_index = html_text.find("var meta = ")
    if marker_index < 0:
        return []
    object_text, _ = extract_braced_object(html_text, marker_index)
    if not object_text:
        return []
    try:
        payload = json.loads(object_text)
    except Exception:
        return []
    products = payload.get("products")
    if not isinstance(products, list):
        return []
    offers: List[Dict[str, Any]] = []
    for product in products:
        if not isinstance(product, dict):
            continue
        handle = str(product.get("handle") or "").strip()
        variants = product.get("variants") or []
        if not handle or not isinstance(variants, list):
            continue
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            raw_price = variant.get("price")
            price_gbp = float(raw_price) / 100.0 if isinstance(raw_price, (int, float)) else None
            product_name = str(variant.get("name") or product.get("title") or "").strip()
            offer = collect_live_offer(
                source["name"],
                product_name,
                price_gbp,
                urljoin(source["product_base_url"], handle),
                search_terms,
                requested_pack_count=requested_pack_count,
            )
            if offer:
                offers.append(offer)
    return offers


def parse_wanahong_price(price_block_html: str) -> Optional[float]:
    ins_match = WANAHONG_INS_PRICE_RE.search(price_block_html)
    if ins_match:
        amount = parse_amount(html.unescape(strip_tags(ins_match.group(1))))
        if amount is not None:
            return amount
    plain_text = html.unescape(strip_tags(price_block_html))
    matches = AMOUNT_RE.findall(plain_text)
    if not matches:
        return None
    return parse_amount(matches[-1])


def parse_wanahong_woocommerce_results(
    html_text: str,
    source: Dict[str, str],
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    offers: List[Dict[str, Any]] = []
    for card_html in WANAHONG_PRODUCT_CARD_RE.findall(html_text):
        name_match = WANAHONG_NAME_RE.search(card_html)
        link_match = WANAHONG_LINK_RE.search(card_html)
        price_match = WANAHONG_PRICE_RE.search(card_html)
        if not name_match or not link_match or not price_match:
            continue
        product_name = html.unescape(strip_tags(name_match.group(1))).strip()
        product_url = urljoin(source["product_base_url"], html.unescape(link_match.group(1)).strip())
        price_gbp = parse_wanahong_price(price_match.group(1))
        offer = collect_live_offer(
            source["name"],
            product_name,
            price_gbp,
            product_url,
            search_terms,
            requested_pack_count=requested_pack_count,
        )
        if offer:
            offers.append(offer)
    return offers


def trolley_retailer_from_store_class(store_class: str) -> str:
    normalized = normalize_retailer_key(store_class.replace("-", " "))
    mapped = TROLLEY_STORE_CLASS_MAP.get(normalized)
    if mapped:
        return mapped
    return retailer_display_name(store_class)


def parse_trolley_product_page_results(
    html_text: str,
    product_url: str,
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    brand_match = TROLLEY_BRAND_RE.search(html_text)
    desc_match = TROLLEY_DESC_RE.search(html_text)
    brand = html.unescape(brand_match.group(1)).strip() if brand_match else ""
    description = html.unescape(desc_match.group(1)).strip() if desc_match else ""
    product_name = " ".join(part for part in (brand, description) if part).strip()
    if not product_name:
        product_name = str(extract_from_title(html_text).get("product_name") or "").strip()
    offers: List[Dict[str, Any]] = []
    for store_class, price_text in TROLLEY_OFFER_RE.findall(html_text):
        retailer = trolley_retailer_from_store_class(store_class)
        offer = collect_live_offer(
            retailer,
            product_name,
            parse_amount(price_text),
            product_url,
            search_terms,
            requested_pack_count=requested_pack_count,
        )
        if offer is None:
            continue
        offer["lookup_source_name"] = "Trolley supermarket comparison"
        offer["lookup_source_url"] = TROLLEY_BASE_URL
        offers.append(offer)
    return offers


def parse_trolley_search_results(
    html_text: str,
    source: Dict[str, str],
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    product_paths = ordered_unique(TROLLEY_PRODUCT_LINK_RE.findall(html_text))
    if not product_paths:
        return []
    offers: List[Dict[str, Any]] = []
    for product_path in product_paths[:TROLLEY_PRODUCT_FETCH_LIMIT]:
        product_url = urljoin(source["product_base_url"], html.unescape(product_path).strip())
        try:
            product_html, final_url = fetch_url(product_url)
        except Exception:
            continue
        offers.extend(
            parse_trolley_product_page_results(
                product_html,
                str(final_url or product_url).strip(),
                search_terms,
                requested_pack_count=requested_pack_count,
            )
        )
    return offers


def extract_live_product_page_data(url: str) -> Dict[str, Any]:
    try:
        html_text, final_url = fetch_url(url)
    except Exception:
        return {}
    merged = merge_values(
        extract_from_jsonld(html_text),
        extract_from_variations(html_text),
        extract_from_meta(html_text),
        extract_from_title(html_text),
    )
    current_price = parse_amount(merged.get("current_price"))
    if current_price is None:
        low_price = parse_amount(merged.get("low_price"))
        high_price = parse_amount(merged.get("high_price"))
        if low_price is not None and high_price is not None and abs(low_price - high_price) < 1e-9:
            current_price = low_price
    price_unit_gbp, unit = extract_standard_unit_price_from_text(html_text)
    if price_unit_gbp is None:
        price_unit_gbp, unit = derive_standard_unit_price(current_price, str(merged.get("product_name") or ""))
    return {
        "product_name": str(merged.get("product_name") or "").strip(),
        "product_url": str(merged.get("canonical_url") or final_url or url).strip(),
        "price_gbp": current_price,
        "price_unit_gbp": price_unit_gbp,
        "unit": unit,
    }


def parse_costco_rest_results(
    payload: Any,
    source: Dict[str, str],
    search_terms: List[str],
    requested_pack_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    products = payload.get("products")
    if not isinstance(products, list):
        return []
    candidates: List[Dict[str, Any]] = []
    for product in products:
        if not isinstance(product, dict):
            continue
        product_name = str(product.get("name") or "").strip()
        if not product_name:
            continue
        normalized_name = normalize_text(product_name)
        score = matching_term_score(
            normalized_name,
            search_terms,
            product_name=product_name,
            requested_pack_count=requested_pack_count,
        )
        if score <= 0:
            continue
        if any(term in normalized_name for term in MERCHANT_EXCLUDED_TERMS if term not in search_terms):
            continue
        price_info = product.get("price")
        price_gbp = None
        if isinstance(price_info, dict):
            price_gbp = parse_amount(price_info.get("value"))
            if price_gbp is None:
                price_gbp = parse_amount(price_info.get("formattedValue"))
        product_path = str(product.get("url") or "").strip()
        candidates.append(
            {
                "product_name": product_name,
                "product_url": urljoin(source["product_base_url"], product_path),
                "price_gbp": price_gbp,
                "score": score,
            }
        )
    if not candidates:
        return []
    ranked_candidates = sorted(
        candidates,
        key=lambda candidate: (
            -int(candidate.get("score", 0)),
            candidate.get("price_gbp") is None,
            str(candidate.get("product_name") or ""),
        ),
    )[:MERCHANT_PRODUCT_PAGE_FETCH_LIMIT]
    offers: List[Dict[str, Any]] = []
    for candidate in ranked_candidates:
        product_name = str(candidate.get("product_name") or "").strip()
        product_url = str(candidate.get("product_url") or "").strip()
        price_gbp = parse_amount(candidate.get("price_gbp"))
        price_unit_gbp, unit = derive_standard_unit_price(price_gbp, product_name)
        if price_gbp is None and product_url:
            page_data = extract_live_product_page_data(product_url)
            if page_data.get("product_name"):
                product_name = str(page_data["product_name"])
            if page_data.get("product_url"):
                product_url = str(page_data["product_url"])
            price_gbp = parse_amount(page_data.get("price_gbp"))
            price_unit_gbp = parse_amount(page_data.get("price_unit_gbp"))
            unit = str(page_data.get("unit") or "").strip()
        offer = collect_live_offer(
            source["name"],
            product_name,
            price_gbp,
            product_url,
            search_terms,
            price_unit_gbp=price_unit_gbp,
            unit=unit,
            requested_pack_count=requested_pack_count,
        )
        if offer:
            offers.append(offer)
    return offers


def find_live_merchant_offers(plan: Dict[str, Any], search_terms: List[str]) -> List[Dict[str, Any]]:
    if not should_try_live_merchant_lookup(plan, search_terms):
        return []
    broad_term_query = not plan.get("retailers") and len(search_terms) == 1 and search_terms[0] in BROAD_ITEM_TERMS
    query = quote_plus(" ".join(search_terms))
    parsers = {
        "trolley_search_html": parse_trolley_search_results,
        "wlfdn_shopify": parse_wlfdn_shopify_results,
        "shopify_meta": parse_shopify_meta_results,
        "costco_rest_json": parse_costco_rest_results,
        "wanahong_woocommerce": parse_wanahong_woocommerce_results,
    }
    best_by_retailer: Dict[str, Dict[str, Any]] = {}
    for source in MERCHANT_SEARCH_SOURCES:
        source_retailer = str(source.get("retailer") or source.get("name") or "").strip()
        supports_retailer_lookup = bool(source.get("supports_retailer_lookup"))
        if plan.get("retailers") and source_retailer and not retailer_requested(plan, source_retailer) and not supports_retailer_lookup:
            continue
        if broad_term_query and not bool(source.get("allow_broad_terms", False)):
            continue
        if not plan.get("retailers") and len(search_terms) < int(source.get("min_search_terms", 1)):
            continue
        parser = parsers.get(source.get("parser", ""))
        if parser is None:
            continue
        search_url = source["search_url"].format(query=query)
        parsed_offers: List[Dict[str, Any]] = []
        try:
            if source.get("response_type") == "json":
                payload, _ = fetch_json_url(search_url)
                parsed_offers = parser(payload, source, search_terms, plan.get("requested_pack_count"))
            else:
                html_text, _ = fetch_url(search_url)
                parsed_offers = parser(html_text, source, search_terms, plan.get("requested_pack_count"))
        except Exception:
            continue
        for offer in parsed_offers:
            retailer = str(offer.get("retailer") or "Unknown retailer")
            if not retailer_requested(plan, retailer):
                continue
            best_by_retailer[retailer] = better_retailer_offer(best_by_retailer.get(retailer), offer)
    if not best_by_retailer:
        return []
    ranked = sorted(
        best_by_retailer.values(),
        key=lambda offer: (
            -int(offer.get("score", 0)),
            offer_metric(offer),
            str(offer.get("retailer") or ""),
            str(offer.get("product_name") or ""),
        ),
    )[:COMMUNITY_LOOKUP_MAX_OFFERS]
    return annotate_lookup_source(
        ranked,
        source_key="retailer_search_pages",
        source_name="Retailer search pages",
        source_url="",
    )


def csv_history_dataset_key(csv_path: Path) -> str:
    return f"{CSV_HISTORY_DATASET_PREFIX}{str(csv_path.expanduser().resolve())}"


def csv_history_fingerprint(csv_path: Path) -> str:
    stat = csv_path.stat()
    payload = f"{csv_path.expanduser().resolve()}:{stat.st_size}:{stat.st_mtime_ns}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def latest_csv_history_fingerprint(connection: sqlite3.Connection, dataset_key: str) -> str:
    row = connection.execute(
        "SELECT source_fingerprint FROM history_import_state WHERE dataset_key = ?",
        (dataset_key,),
    ).fetchone()
    if row is None:
        return ""
    return str(row["source_fingerprint"] or "")


def ensure_csv_history_migrated(csv_path: Path) -> str:
    dataset_key = csv_history_dataset_key(csv_path)
    connection = open_cache_db()
    now = int(time.time())
    try:
        latest_fingerprint = latest_csv_history_fingerprint(connection, dataset_key)
        if not csv_path.exists():
            return latest_fingerprint
        fingerprint = csv_history_fingerprint(csv_path)
        if fingerprint == latest_fingerprint:
            return fingerprint
        source_name = SOURCE_MAP["community_supermarket_dataset"].name
        source_url = SOURCE_MAP["community_supermarket_dataset"].url
        row_count = 0
        with csv_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for line_number, row in enumerate(reader, start=2):
                product_name = str(row.get("product_name") or "").strip()
                normalized_name = str(row.get("normalized_product_name") or normalize_text(product_name)).strip()
                retailer = retailer_display_name(str(row.get("supermarket_name") or row.get("retailer") or "").strip())
                metadata = {
                    "line_number": line_number,
                    "source_path": str(csv_path),
                }
                record_hash = hashlib.sha256(
                    f"csv_snapshot:{dataset_key}:{fingerprint}:{line_number}".encode("utf-8")
                ).hexdigest()
                connection.execute(
                    """
                    INSERT OR IGNORE INTO price_history (
                        origin,
                        dataset_key,
                        dataset_fingerprint,
                        namespace,
                        cache_key,
                        request_fingerprint,
                        retailer,
                        product_name,
                        normalized_product_name,
                        price_gbp,
                        price_unit_gbp,
                        unit,
                        capture_date,
                        category_name,
                        is_own_brand,
                        product_url,
                        source_key,
                        source_name,
                        source_url,
                        fetched_at,
                        expires_at,
                        observed_at,
                        record_hash,
                        raw_offer_json,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "csv_snapshot",
                        dataset_key,
                        fingerprint,
                        "",
                        "",
                        "",
                        retailer,
                        product_name,
                        normalized_name,
                        parse_amount(row.get("price_gbp")),
                        parse_amount(row.get("price_unit_gbp")),
                        str(row.get("unit") or "").strip(),
                        str(row.get("capture_date") or "").strip(),
                        str(row.get("category_name") or "").strip().lower(),
                        1 if str(row.get("is_own_brand") or "").strip().lower() in {"1", "true", "yes"} else 0,
                        "",
                        "community_supermarket_dataset",
                        source_name,
                        source_url,
                        0,
                        0,
                        now,
                        record_hash,
                        json.dumps(row, sort_keys=True),
                        json.dumps(metadata, sort_keys=True),
                    ),
                )
                row_count += 1
        connection.execute(
            """
            INSERT INTO history_import_state (dataset_key, source_path, source_fingerprint, row_count, imported_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(dataset_key) DO UPDATE SET
                source_path = excluded.source_path,
                source_fingerprint = excluded.source_fingerprint,
                row_count = excluded.row_count,
                imported_at = excluded.imported_at
            """,
            (dataset_key, str(csv_path), fingerprint, row_count, now),
        )
        connection.commit()
        return fingerprint
    finally:
        connection.close()


def find_csv_direct_item_offers(plan: Dict[str, Any], search_terms: List[str]) -> List[Dict[str, Any]]:
    dataset_key = csv_history_dataset_key(COMMUNITY_DATASET_CSV_PATH)
    fingerprint = ensure_csv_history_migrated(COMMUNITY_DATASET_CSV_PATH)
    if not fingerprint:
        return []
    primary_term = search_terms[0] if search_terms else ""
    connection = open_cache_db()
    try:
        if primary_term:
            rows = connection.execute(
                """
                SELECT
                    retailer AS supermarket_name,
                    product_name,
                    normalized_product_name,
                    price_gbp,
                    price_unit_gbp,
                    unit,
                    capture_date,
                    category_name,
                    is_own_brand
                FROM price_history
                WHERE origin = 'csv_snapshot'
                  AND dataset_key = ?
                  AND dataset_fingerprint = ?
                  AND normalized_product_name LIKE ?
                """,
                (dataset_key, fingerprint, f"%{primary_term}%"),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT
                    retailer AS supermarket_name,
                    product_name,
                    normalized_product_name,
                    price_gbp,
                    price_unit_gbp,
                    unit,
                    capture_date,
                    category_name,
                    is_own_brand
                FROM price_history
                WHERE origin = 'csv_snapshot'
                  AND dataset_key = ?
                  AND dataset_fingerprint = ?
                """,
                (dataset_key, fingerprint),
            ).fetchall()
    finally:
        connection.close()
    best_by_retailer: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        offer = collect_offer_candidate(dict(row), search_terms, plan.get("requested_pack_count"))
        if not offer or not retailer_requested(plan, str(offer.get("retailer") or "")):
            continue
        retailer = str(offer.get("retailer") or "Unknown retailer")
        best_by_retailer[retailer] = better_retailer_offer(best_by_retailer.get(retailer), offer)
    ranked = sorted(
        best_by_retailer.values(),
        key=lambda offer: (
            -int(offer.get("score", 0)),
            offer_metric(offer),
            -capture_rank(str(offer.get("capture_date") or "")),
            str(offer.get("retailer") or ""),
        ),
    )[:COMMUNITY_LOOKUP_MAX_OFFERS]
    return annotate_lookup_source(
        ranked,
        source_key="community_supermarket_dataset",
        source_name=SOURCE_MAP["community_supermarket_dataset"].name,
        source_url=SOURCE_MAP["community_supermarket_dataset"].url,
    )


def find_direct_item_offers(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not should_try_direct_item_lookup(plan):
        return []
    search_terms = extract_item_terms(plan)
    cached_offers, stale_source_key = load_item_lookup_cache_state(plan, search_terms)
    if cached_offers:
        return cached_offers
    source_fetchers: List[Tuple[str, Any]] = [
        ("retailer_search_pages", lambda: find_live_merchant_offers(plan, search_terms)),
        ("amazon_api_live_offers", lambda: find_amazon_api_offers(plan, search_terms)),
        ("pricesapi_live_offers", lambda: find_pricesapi_offers(plan, search_terms, [])),
    ]
    if stale_source_key:
        source_fetchers.sort(key=lambda item: item[0] == stale_source_key)
    for source_key, fetcher in source_fetchers:
        offers = fetcher()
        if not offers:
            continue
        if stale_source_key and source_key != stale_source_key:
            for offer in offers:
                offer["refreshed_after_stale_source"] = stale_source_key
        store_cached_item_lookup(plan, search_terms, offers)
        return offers
    csv_offers = find_csv_direct_item_offers(plan, search_terms)
    if csv_offers:
        for offer in csv_offers:
            offer["live_lookup_attempted"] = True
    return csv_offers


def direct_lookup_source_key(offers: List[Dict[str, Any]]) -> str:
    if not offers:
        return "community_supermarket_dataset"
    return str(offers[0].get("lookup_source_key") or "community_supermarket_dataset")


def direct_lookup_source_name(offers: List[Dict[str, Any]]) -> str:
    if not offers:
        return SOURCE_MAP["community_supermarket_dataset"].name
    return str(offers[0].get("lookup_source_name") or SOURCE_MAP["community_supermarket_dataset"].name)


def direct_lookup_source_url(offers: List[Dict[str, Any]]) -> str:
    if not offers:
        return SOURCE_MAP["community_supermarket_dataset"].url
    return str(offers[0].get("lookup_source_url") or "")


def direct_lookup_matched_sources(offers: List[Dict[str, Any]]) -> List[str]:
    if not offers:
        return []
    source_key = direct_lookup_source_key(offers)
    if source_key in {"amazon_api_live_offers", "pricesapi_live_offers", "brightdata_google_shopping", "retailer_search_pages"}:
        return ordered_unique(str(offer.get("retailer") or "") for offer in offers if offer.get("retailer"))
    return [SOURCE_MAP["community_supermarket_dataset"].name]


def direct_lookup_comparison_basis(offers: List[Dict[str, Any]]) -> str:
    units = ordered_unique(str(offer.get("unit") or "").strip() for offer in offers if offer.get("price_unit_gbp") is not None and str(offer.get("unit") or "").strip())
    if not units:
        return "shelf price only"
    if len(units) == 1:
        return f"standardized £/{units[0]}, then shelf price"
    return "standardized unit price where available, then shelf price"


def build_direct_item_reply(plan: Dict[str, Any], offers: List[Dict[str, Any]]) -> str:
    terms = extract_item_terms(plan)
    source_key = direct_lookup_source_key(offers)
    source_url = direct_lookup_source_url(offers)
    shortlist_context = offers[0].get("csv_shortlist_retailers") or [] if offers else []
    cache_hit = any(bool(offer.get("cache_hit")) for offer in offers)
    live_lookup_attempted = any(bool(offer.get("live_lookup_attempted")) for offer in offers)
    refreshed_after_stale_source = str(offers[0].get("refreshed_after_stale_source") or "") if offers else ""
    lines = [f"Need: {plan['query_label']}"]
    if terms:
        lines.append(f"Search terms: {', '.join(terms)}")
    if plan.get("retailers"):
        lines.append(f"Retailers mentioned: {', '.join(plan['retailers'])}")
    lines.append("")
    if source_key in {"amazon_api_live_offers", "pricesapi_live_offers", "brightdata_google_shopping"}:
        shortlist = offers[0].get("csv_shortlist_retailers") or []
        missing = offers[0].get("csv_shortlist_missing_retailers") or []
        if source_key in {"pricesapi_live_offers", "brightdata_google_shopping"} and shortlist:
            lines.append(f"CSV-shortlisted retailers: {', '.join(str(item) for item in shortlist)}")
            lines.append("")
        if source_key == "amazon_api_live_offers":
            lines.append("Recent cached live offers from Amazon Product Advertising API:" if cache_hit else "Latest live offers from Amazon Product Advertising API:")
        elif source_key == "pricesapi_live_offers":
            lines.append("Recent cached live offers from PricesAPI:" if cache_hit else "Latest live offers from PricesAPI:")
        else:
            lines.append("Recent cached live offers from Bright Data Google Shopping:" if cache_hit else "Latest live offers from Bright Data Google Shopping:")
        if source_key in {"pricesapi_live_offers", "brightdata_google_shopping"} and missing:
            lines.append(f"Live results not matched for: {', '.join(str(item) for item in missing)}")
    elif source_key == "retailer_search_pages":
        lines.append("Recent cached live offers from retailer search pages:" if cache_hit else "Best matched live offers from retailer search pages:")
    else:
        lines.append("Best retailer matches from community dataset:")
        if live_lookup_attempted:
            lines.append("Live retailer checks ran first but did not return a qualifying current match for this query.")
    if refreshed_after_stale_source:
        lines.append(f"Refreshed after 24-hour cache expiry by trying a different source before {refreshed_after_stale_source}.")
    lines.append(f"Comparison basis: {direct_lookup_comparison_basis(offers)}")
    for idx, offer in enumerate(offers, start=1):
        price_text = format_amount("GBP", offer.get("price_gbp"))
        unit_price = offer.get("price_unit_gbp")
        unit = str(offer.get("unit") or "").strip()
        if unit_price is not None and unit:
            price_text += f" ({format_amount('GBP', unit_price)}/{unit})"
        line = f"{idx}. {offer.get('retailer')}: {offer.get('product_name')} - {price_text}"
        if offer.get("is_own_brand"):
            line += " - own brand"
        capture_date = str(offer.get("capture_date") or "")[:10]
        if capture_date:
            line += f" - captured {capture_date}"
        lines.append(line)
        product_url = str(offer.get("product_url") or "").strip()
        if product_url:
            lines.append(f"   Link: {product_url}")
    lines.append("")
    if source_url:
        lines.append(f"Source: {source_url}")
    else:
        lines.append("Source: live retailer search pages")
    lines.append("Caveats:")
    if source_key == "amazon_api_live_offers":
        lines.extend(
            [
                "- Reused a recent cached item lookup for the same query to reduce repeated API calls; prices may have changed since that fetch." if cache_hit else "- Amazon offers come from the Amazon Product Advertising API (PA-API), not direct HTML scraping.",
                "- Availability, shipping eligibility, and final checkout pricing can still differ by account and location.",
                "- Verify the Amazon product page before checkout if pack size or seller matters.",
            ]
        )
    elif source_key == "pricesapi_live_offers":
        lines.extend(
            [
                "- Reused a recent cached item lookup for the same query to reduce repeated API calls; prices may have changed since that fetch." if cache_hit else "- The retailer shortlist comes from internal history records, then PricesAPI checks live offers for those shortlisted retailers when it can match them." if shortlist_context else "- PricesAPI checks live offers directly from your query terms when no retailer shortlist is available.",
                "- PricesAPI uses its own product catalog and seller coverage, so some UK grocery retailers may not appear even if they were shortlisted from internal history.",
                "- Verify the retailer page before checkout if the exact pack size or delivery terms matter.",
            ]
        )
    elif source_key == "brightdata_google_shopping":
        lines.extend(
            [
                "- Reused a recent cached item lookup for the same query to reduce repeated API calls; prices may have changed since that fetch." if cache_hit else "- The retailer shortlist comes from the local Kaggle-derived CSV snapshot, then Bright Data checks fresher Google Shopping offers for those retailers.",
                "- Coverage depends on Google Shopping and merchant-feed availability, so some shortlisted retailers may not return a live match.",
                "- Verify the retailer page before checkout if the exact pack size or delivery terms matter.",
            ]
        )
    elif source_key == "retailer_search_pages":
        lines.extend(
            [
                "- Reused a recent cached item lookup for the same query to reduce repeated crawling; prices may have changed since that fetch." if cache_hit else "- These are live retailer search-page matches, so availability and pricing can change between search and checkout.",
                "- Result quality depends on each retailer site matching the item terms you sent.",
                "- Send a public product page URL if you want an exact live page-price extraction for one specific item.",
            ]
        )
    else:
        lines.extend(
            [
                "- Uses internal SQLite history records migrated from the Kaggle-derived supermarket CSV snapshot, not an official statistic.",
                "- These are the latest matched capture rows in the internal history snapshot, not guaranteed live shelf prices today.",
                "- Send a public product page URL if you want an exact live page-price extraction.",
            ]
        )
    return "\n".join(lines).strip()


def normalize_postcode(postcode: str) -> str:
    compact = WHITESPACE_RE.sub("", (postcode or "").upper())
    if len(compact) <= 3:
        return compact
    return f"{compact[:-3]} {compact[-3:]}"


def extract_search_anchor(text: str, focus_terms: List[str], query_type: str) -> Tuple[str, bool]:
    if query_type != "store_location":
        return "", False
    postcode_match = UK_POSTCODE_RE.search(text or "")
    if postcode_match:
        return normalize_postcode(postcode_match.group(1)), False
    if focus_terms:
        return " ".join(term.capitalize() for term in focus_terms[:2]), False
    return DEFAULT_LOCATION_POSTCODE, True


def normalize_retailer_mentions(text: str) -> str:
    normalized = re.sub(r"\bm\s*&\s*s\b", "mands", text or "")
    normalized = re.sub(r"\bmarks\s*(?:&|and)\s*spencer\b", "marksandspencer", normalized)
    return normalized


def classify_query(text: str) -> Dict[str, Any]:
    lowered = normalize_retailer_mentions((text or "").lower())
    tokens = WORD_RE.findall(lowered)
    token_set = set(tokens)
    retailers = ordered_unique(RETAILER_ALIASES[token] for token in tokens if token in RETAILER_ALIASES)
    official_only = bool(token_set & OFFICIAL_ONLY_KEYWORDS)
    if token_set & LOCATION_KEYWORDS:
        query_type = "store_location"
    elif token_set & BASKET_KEYWORDS:
        query_type = "basket_affordability"
    elif token_set & TREND_KEYWORDS:
        query_type = "inflation_trend"
    elif token_set & VALUE_KEYWORDS or retailers:
        query_type = "retailer_comparison"
    else:
        query_type = "item_price_lookup"
    needs_live = bool(token_set & LIVE_PRICE_KEYWORDS) or query_type in {"retailer_comparison", "item_price_lookup"}
    requested_pack_count = extract_requested_pack_count(text)
    noise = QUERY_NOISE | set(RETAILER_ALIASES.keys()) | BASKET_KEYWORDS | TREND_KEYWORDS | LOCATION_KEYWORDS | VALUE_KEYWORDS | LIVE_PRICE_KEYWORDS | OFFICIAL_ONLY_KEYWORDS
    focus_terms = ordered_unique(token for token in tokens if len(token) > 2 and token not in noise and not token.isdigit())[:4]
    search_anchor, used_default_postcode = extract_search_anchor(text, focus_terms, query_type)
    return {
        "query_type": query_type,
        "query_label": QUERY_LABELS[query_type],
        "official_only": official_only,
        "needs_live": needs_live,
        "retailers": retailers,
        "focus_terms": focus_terms,
        "requested_pack_count": requested_pack_count,
        "search_anchor": search_anchor,
        "used_default_postcode": used_default_postcode,
        "text": text.strip(),
    }


def source_score(source: FoodPriceSource, plan: Dict[str, Any]) -> int:
    score = 0
    query_type = plan["query_type"]
    if query_type in source.best_for:
        score += 45
    if plan["official_only"]:
        score += 20 if source.official else -35
    elif not source.official:
        score += 5
    if plan["needs_live"]:
        if source.retailer_level and not source.historical_only and not source.location_only:
            score += 18
        elif source.official:
            score += 4
        if source.key == "ons_shopping_tool":
            score += 6
        if source.historical_only:
            score -= 25
    if plan["retailers"] and source.retailer_level:
        score += 8
    if query_type == "basket_affordability" and source.key == "food_foundation_basic_basket":
        score += 20
    if query_type == "inflation_trend" and source.key == "ons_shopping_tool":
        score += 12
    if query_type == "retailer_comparison" and source.key == "community_supermarket_dataset":
        score += 20
    if query_type == "retailer_comparison" and source.key == "ons_shopping_tool":
        score += 15
    if query_type == "store_location" and source.key == "geolytix_locations":
        score += 25
    if query_type != "store_location" and source.location_only:
        score -= 30
    return score


def select_sources(plan: Dict[str, Any]) -> List[FoodPriceSource]:
    ranked = sorted(FOOD_PRICE_SOURCES, key=lambda source: (source_score(source, plan), -FOOD_PRICE_SOURCES.index(source)), reverse=True)
    selected = [source for source in ranked if source_score(source, plan) > 0][:3]
    if plan["official_only"]:
        official_selected = [source for source in selected if source.official]
        if official_selected:
            selected = official_selected
        else:
            selected = [SOURCE_MAP["ons_shopping_tool"], SOURCE_MAP["gov_food_statistics"]]
    if not selected:
        selected = [SOURCE_MAP["ons_shopping_tool"], SOURCE_MAP["food_foundation_basic_basket"]]
    return selected


def primary_reason(source: FoodPriceSource, plan: Dict[str, Any]) -> str:
    query_type = plan["query_type"]
    if source.key == "community_supermarket_dataset":
        return "Retailer-level scraped prices and price-per-unit data from the Kaggle-derived supermarket dataset make this the strongest fit for supermarket-to-supermarket value checks."
    if source.key == "ons_shopping_tool":
        if query_type == "inflation_trend":
            return "It is the strongest official source for average item price trends across the UK consumer basket."
        return "It is the strongest official source for tracking how a named basket item moves over time."
    if source.key == "food_foundation_basic_basket":
        return "It is the clearest weekly signal for whether a healthy basket is becoming more or less affordable."
    if source.key == "ons_online_weekly_changes":
        return "It gives historical weekly movement from scraped retailer websites, which is useful when you care about direction of travel rather than a single live shelf price."
    if source.key == "gov_food_statistics":
        return "It provides the cleanest official annual context on food prices, spending, and household pressure."
    if source.key == "geolytix_locations":
        return "It is useful when the first job is finding which supermarket branches are nearby before checking prices elsewhere."
    return source.description


def secondary_reason(source: FoodPriceSource) -> str:
    if source.key == "community_supermarket_dataset":
        return "Kaggle-derived retailer-level and value-per-unit comparison."
    if source.key == "ons_shopping_tool":
        return "Official average item prices over time."
    if source.key == "food_foundation_basic_basket":
        return "Weekly nutritious basket affordability."
    if source.key == "ons_online_weekly_changes":
        return "Historical weekly retailer price movements."
    if source.key == "gov_food_statistics":
        return "Annual official food price context."
    if source.key == "geolytix_locations":
        return "Nearby supermarket locations only."
    return source.description


def build_query_caveats(plan: Dict[str, Any], sources: List[FoodPriceSource]) -> List[str]:
    caveats: List[str] = []
    if plan["needs_live"]:
        caveats.append("Official ONS and GOV sources track averages or trends rather than a live shelf-price feed.")
    if any(source.key == "ons_online_weekly_changes" for source in sources):
        caveats.append("ONS Online Weekly Price Changes are experimental historical data and stop in 2021.")
    if any(source.location_only for source in sources):
        caveats.append("Geolytix helps locate stores, but it does not publish prices.")
    if any(not source.official for source in sources):
        caveats.append("Community and civil-society datasets are operationally useful, but they are not official statistics.")
    if plan["official_only"] and plan["query_type"] in {"retailer_comparison", "store_location"}:
        caveats.append("This catalog does not contain an official live retailer-by-retailer shelf-price source.")
    return ordered_unique(caveats)[:3]


def build_query_reply(plan: Dict[str, Any], sources: List[FoodPriceSource]) -> str:
    primary = sources[0]
    lines = [f"Need: {plan['query_label']}"]
    if plan["focus_terms"]:
        lines.append(f"Focus: {', '.join(plan['focus_terms'])}")
    if plan["retailers"]:
        lines.append(f"Retailers mentioned: {', '.join(plan['retailers'])}")
    if plan["query_type"] == "store_location" and plan["search_anchor"]:
        anchor_label = "Default postcode" if plan["used_default_postcode"] else "Search anchor"
        lines.append(f"{anchor_label}: {plan['search_anchor']}")
    lines.extend(
        [
            "",
            f"Primary source: {primary.name}",
            f"Why: {primary_reason(primary, plan)}",
            f"Link: {primary.url}",
        ]
    )
    if len(sources) > 1:
        lines.append("")
        lines.append("Also useful:")
        for source in sources[1:]:
            lines.append(f"- {source.name}: {secondary_reason(source)}")
            lines.append(f"  {source.url}")
    caveats = build_query_caveats(plan, sources)
    if caveats:
        lines.append("")
        lines.append("Caveats:")
        for item in caveats:
            lines.append(f"- {item}")
    if plan["needs_live"]:
        lines.append("")
        lines.append("Tip: Send a public supermarket product URL if you want an exact current page-price extraction.")
    return "\n".join(lines).strip()


def build_query_result(raw_text: str) -> PriceResult:
    plan = classify_query(raw_text)
    direct_offers = find_direct_item_offers(plan)
    if direct_offers:
        summary_terms = ", ".join(extract_item_terms(plan)) or plan["query_label"]
        return PriceResult(
            ok=True,
            summary=f"Direct retailer matches for {summary_terms}",
            source=direct_lookup_source_name(direct_offers),
            canonical_url=direct_lookup_source_url(direct_offers),
            error_message="",
            reply_message=build_direct_item_reply(plan, direct_offers),
            mode="query",
            query_type=plan["query_type"],
            matched_sources=direct_lookup_matched_sources(direct_offers),
        )
    sources = select_sources(plan)
    primary = sources[0]
    summary = f"UK food price guide: {plan['query_label']} -> {primary.name}"
    return PriceResult(
        ok=True,
        summary=summary,
        source=primary.name,
        canonical_url=primary.url,
        error_message="",
        reply_message=build_query_reply(plan, sources),
        mode="query",
        query_type=plan["query_type"],
        matched_sources=[source.name for source in sources],
    )


def analyze_product_payload(raw_text: str) -> PriceResult:
    try:
        source = resolve_source(raw_text)
    except Exception as exc:
        return error_result(None, str(exc))

    try:
        merged = merge_values(
            extract_from_jsonld(source.content),
            extract_from_variations(source.content),
            extract_from_meta(source.content),
            extract_from_known_retailer_html(source.content, source.canonical_url or source.source_label),
            extract_from_title(source.content),
            extract_amounts_from_text(source.content),
        )
        return build_product_result(source, merged)
    except requests.RequestException as exc:
        return error_result(source, f"failed to fetch page: {exc}")
    except Exception as exc:
        return error_result(source, f"pipeline error: {exc}")


def analyze_payload(raw_text: str) -> PriceResult:
    text = (raw_text or "").strip()
    if not text:
        return error_result(None, "send a UK food price question, a public product page URL, or pasted HTML")
    if should_use_product_extraction(text):
        return analyze_product_payload(text)
    return build_query_result(text)


def result_to_dict(result: PriceResult) -> Dict[str, Any]:
    return {
        "ok": result.ok,
        "summary": result.summary,
        "source": result.source,
        "product_name": result.product_name,
        "canonical_url": result.canonical_url,
        "currency": result.currency,
        "current_price": result.current_price,
        "regular_price": result.regular_price,
        "low_price": result.low_price,
        "high_price": result.high_price,
        "regular_low_price": result.regular_low_price,
        "regular_high_price": result.regular_high_price,
        "availability": result.availability,
        "discount_percent": result.discount_percent,
        "error_message": result.error_message,
        "reply_message": result.reply_message,
        "mode": result.mode,
        "query_type": result.query_type,
        "matched_sources": result.matched_sources,
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze UK food price queries or extract product page prices")
    parser.add_argument("input", nargs="?", default="", help="Food price query, URL, local HTML file path, or raw HTML")
    parser.add_argument("--stdin", action="store_true", help="Read payload from stdin")
    parser.add_argument("--json", action="store_true", help="Print JSON result")
    parser.add_argument("--json-brief", action="store_true", help="Alias for JSON response")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    raw_text = read_stdin() if args.stdin else args.input
    result = analyze_payload(raw_text)
    if args.json or args.json_brief:
        print(json.dumps(result_to_dict(result), ensure_ascii=False))
    else:
        print(result.reply_message if result.reply_message else result.summary)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
