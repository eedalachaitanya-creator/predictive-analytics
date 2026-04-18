"""
scout/scraper.py — Dynamic scraper for Scout Agent

Extraction pipeline per site:
  1. requests → search page → find + validate product → requests → product page
  2. Playwright → search page (if requests blocked or site in PLAYWRIGHT_ONLY_DOMAINS)
  3. ScraperAPI → search page (if Playwright is also blocked — e.g. Walmart)
  4. Playwright → product page (if requests blocked on product page)

Key design principles:
  - No site-specific hardcoding in the scrape flow
  - Playwright context locale/timezone derived from domain TLD, not hardcoded names
  - Product card extraction uses fully generic selectors — no per-site if/else blocks
  - Playwright wait uses one unified generic selector list for all sites
  - Fallback URL pattern detected from site's search_url template, not hardcoded
  - OpenAI product validation has retry + heuristic fallback
  - All product details stored inside product_details JSON
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

from .resolver import build_search_url
from .entity_resolver import extract_brand_from_title

# ── Logging ───────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── Config ────────────────────────────────────────────────────────────

OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
SCRAPER_API_KEY  = os.getenv("SCRAPER_API_KEY", "")   # optional — used as fallback when Playwright is blocked

# Use Langfuse-instrumented OpenAI client for automatic cost tracking
from .langfuse_config import get_openai_client as _get_langfuse_openai


# ── OpenAI cost tracking ─────────────────────────────────────────────
# Wraps every OpenAI call with terminal logging: endpoint, tokens, cost.
# Works regardless of Langfuse being enabled or disabled.

_COST_PER_1K = {
    # Input / Output per 1K tokens (USD)
    "gpt-4o-mini": (0.00015, 0.0006),
    "gpt-4o":      (0.005,   0.015),
    "gpt-4":       (0.03,    0.06),
    "gpt-3.5-turbo": (0.0005, 0.0015),
}

import threading as _cost_threading
_total_cost_usd = 0.0
_total_calls = 0
_cost_lock = _cost_threading.Lock()


class _CostTrackingClient:
    """Wraps OpenAI client to log cost of every call to terminal."""

    def __init__(self, client):
        self._client = client
        self.chat = self  # so .chat.completions.create works

    @property
    def completions(self):
        return self

    def create(self, **kwargs):
        global _total_cost_usd, _total_calls
        model = kwargs.get("model", "gpt-4o-mini")
        response = self._client.chat.completions.create(**kwargs)

        # Extract usage
        usage = response.usage
        if usage:
            input_tok = usage.prompt_tokens
            output_tok = usage.completion_tokens
            total_tok = usage.total_tokens

            rates = _COST_PER_1K.get(model, (0.00015, 0.0006))
            cost = (input_tok / 1000 * rates[0]) + (output_tok / 1000 * rates[1])

            with _cost_lock:
                _total_cost_usd += cost
                _total_calls += 1
                cumulative = _total_cost_usd

            # Determine call purpose from prompt content
            messages = kwargs.get("messages", [])
            prompt_preview = ""
            for m in messages:
                content = m.get("content", "")
                if "yes" in content.lower() and "no" in content.lower() and "same product" in content.lower():
                    prompt_preview = "product_validation"
                    break
                elif "manufacturer" in content.lower() and "json" in content.lower():
                    prompt_preview = "manufacturer_extract"
                    break
            if not prompt_preview:
                prompt_preview = messages[-1].get("content", "")[:40] if messages else "unknown"

            logger.info(
                f"💲 OpenAI [{prompt_preview}] "
                f"model={model} tokens={total_tok} "
                f"(in={input_tok} out={output_tok}) "
                f"cost=${cost:.4f} cumulative=${cumulative:.4f}"
            )

        return response


# Wrap the client
_cost_tracked_client = None

def _openai():
    """Get the cost-tracking OpenAI client, initializing it on first call."""
    global _cost_tracked_client
    if _cost_tracked_client is None:
        raw = _get_langfuse_openai()
        if raw:
            _cost_tracked_client = _CostTrackingClient(raw)
    return _cost_tracked_client

TIMEOUT_REQ      = 10
TIMEOUT_SCRAPER  = 45
MAX_PAGE_CHARS   = 20000

# ── Adaptive scraping state ───────────────────────────────────────────
# Instead of hardcoding which sites block requests or Playwright,
# we track failures at runtime and adapt. Sites that block requests
# get auto-escalated to Playwright. Sites that block Playwright
# get auto-escalated to ScraperAPI.

_site_transport_hints: dict[str, str] = {}
# Values: "requests_ok", "requests_blocked", "playwright_blocked"
# Populated dynamically as scrapes succeed or fail.

import threading as _threading
_hints_lock = _threading.Lock()

# URL params that are tracking noise — safe to strip
_STRIP_PARAM_PATTERNS = [
    r"^utm_", r"^ref$", r"^crid$", r"^sprefix$",
    r"^gclid$", r"^fbclid$", r"^msclkid$",
    r"^pf_rd_", r"^pd_rd_", r"^hydadcr$",
    r"^smid$", r"^tag$", r"^otracker",
]

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
]

_FOOD_SIGNALS = frozenset([
    "supplement facts", "nutrition facts", "nutritional information",
    "calories", "protein", "carbohydrate", "total fat", "sodium",
    "serving size", "servings per", "daily value",
    "vitamin", "mineral", "probiotic", "ayurvedic", "herbal",
    "capsule", "tablet", "softgel", "gummy", "powder",
    "ingredient", "supplement", "nutraceutical",
])

# ── Locale helpers (dynamic — derived from URL, no hardcoding) ────────

# ── Currency/locale registry ──────────────────────────────────────────
# Instead of hardcoding Indian domains, we detect locale from:
#   1. Explicit currency hint registered at scrape-time (from DB site data)
#   2. TLD-based detection (.in, .co.in → INR)
#   3. Homepage HTML currency signals (₹ symbol prevalence)
# Sites register themselves when _scrape_sync starts, so by the time
# _locale_for_url is called for the product page, the hint is already set.

_currency_hints: dict[str, str] = {}  # host → "INR", "USD", etc.
_currency_lock = threading.Lock()

# ── Playwright launch serialization ─────────────────────────────────
# Playwright's chromium.launch() can fail with "spawn UNKNOWN" on Windows
# when 5+ browsers try to start simultaneously — filesystem races on temp
# dirs, IPC pipes, and user-data-dir creation. We allow up to 2 concurrent
# launches: safely below the failure threshold, and once browsers are
# running, navigation and extraction proceed fully in parallel.
# If "spawn UNKNOWN" still shows in logs, reduce Semaphore(2) to Semaphore(1).
_playwright_launch_semaphore = threading.Semaphore(2)


def register_currency_hint(base_url: str, currency: str) -> None:
    """Register a currency hint for a site's domain. Called from _scrape_sync."""
    host = urllib.parse.urlparse(base_url).netloc.lower()
    with _currency_lock:
        _currency_hints[host] = currency.upper()


def _extract_all_domain_parts(host: str) -> set[str]:
    """
    Extract all meaningful parts from a hostname.
    'shop.beatoapp.com' → {'shop', 'beatoapp'}
    'www.myntra.com' → {'myntra'}
    """
    # Strip known TLDs first
    clean = host
    for suffix in [".co.in", ".com.au", ".co.uk", ".com", ".in", ".net", ".io", ".co", ".org"]:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)]
            break
    parts = {p for p in clean.split(".") if p and p != "www" and p != "m"}
    return parts


# Known Indian .com domains — this is a SEED list. The real detection
# is dynamic via currency hints and HTML analysis. This just handles
# the first-visit case before we've seen the site's HTML.
_INDIAN_DOMAIN_SEEDS = {
    "myntra", "flipkart", "meesho", "nykaa", "nykaafashion",
    "ajio", "tatacliq", "croma", "jiomart", "bigbasket",
    "snapdeal", "healthkart", "pharmeasy", "netmeds",
    "1mg", "tata1mg", "apollopharmacy", "beatoapp", "beato",
    "zomato", "swiggy", "blinkit", "zepto", "dunzo",
    "purplle", "mamaearth", "wow", "plixlife", "lenskart",
    "tanishq", "titan", "boat", "boatlifestyle", "noise",
    "gonduit", "gonoise",
}


def _locale_for_url(url: str) -> tuple[str, str, str]:
    """
    Return (locale, timezone_id, accept_language) based on the URL's TLD,
    registered currency hints, and domain name signals.
    
    Detection priority:
      1. Explicit currency hint (registered by _scrape_sync from DB data)
      2. TLD-based (.in, .co.in → INR)
      3. Domain name seed list (for .com Indian sites on first visit)
      4. Default to US locale
    """
    _INR_LOCALE = ("en-IN", "Asia/Kolkata", "en-IN,en;q=0.9")

    _TLD_MAP = {
        ".co.in":  _INR_LOCALE,
        ".in":     _INR_LOCALE,
        ".co.uk":  ("en-GB", "Europe/London",       "en-GB,en;q=0.9"),
        ".com.au": ("en-AU", "Australia/Sydney",     "en-AU,en;q=0.9"),
        ".ca":     ("en-CA", "America/Toronto",      "en-CA,en;q=0.9"),
        ".de":     ("de-DE", "Europe/Berlin",        "de-DE,de;q=0.9"),
        ".fr":     ("fr-FR", "Europe/Paris",         "fr-FR,fr;q=0.9"),
        ".jp":     ("ja-JP", "Asia/Tokyo",           "ja-JP,ja;q=0.9"),
        ".sg":     ("en-SG", "Asia/Singapore",       "en-SG,en;q=0.9"),
        ".ae":     ("en-AE", "Asia/Dubai",           "en-AE,en;q=0.9"),
    }
    parsed = urllib.parse.urlparse(url)
    host   = parsed.netloc.lower()

    # Priority 1: Explicit currency hint from DB/registration
    with _currency_lock:
        hint = _currency_hints.get(host, "")
    if hint == "INR":
        return _INR_LOCALE

    # Priority 2: TLD-based detection (check longer TLDs first)
    for tld, vals in _TLD_MAP.items():
        if host.endswith(tld):
            return vals

    # Priority 3: For .com domains, check if ANY domain part matches Indian seeds
    if host.endswith(".com") or host.endswith(".co"):
        domain_parts = _extract_all_domain_parts(host)
        if domain_parts & _INDIAN_DOMAIN_SEEDS:
            return _INR_LOCALE

    # Default: US locale
    return ("en-US", "America/New_York", "en-US,en;q=0.9")


def _needs_http2_disabled(url: str) -> bool:
    """
    Detect if HTTP/2 should be disabled for this URL.
    Uses runtime failure tracking instead of a hardcoded domain list.
    On first visit, defaults to disabled (safer) — HTTP/2 issues are
    common with headless browsers and the perf difference is negligible.
    """
    # Default: disable HTTP/2 for all headless fetches — it causes
    # more problems than it solves and the speed difference is minimal
    # for scraping. This eliminates the need for a hardcoded list.
    return True


# ── Public entry point ────────────────────────────────────────────────

async def scrape_product_on_site(site: dict, product_name: str) -> dict:
    """Async entry point. Runs the sync scrape in a thread pool."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = await loop.run_in_executor(executor, _scrape_sync, site, product_name)
    return result


# ── URL helpers ───────────────────────────────────────────────────────

def _clean_product_url(url: str) -> str:
    """Strip tracking params, keep variant/size params."""
    parsed = urllib.parse.urlparse(url)
    if not parsed.query:
        return url
    kept = [
        (k, v) for k, v in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if not any(re.search(p, k, re.IGNORECASE) for p in _STRIP_PARAM_PATTERNS)
    ]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(kept)))


def _build_fallback_url(base_url: str, product_name: str, search_url_template: str) -> Optional[str]:
    """
    Dynamically derive a fallback product URL from the site's URL patterns.
    No hardcoded domain names — entirely based on URL structure analysis.
    
    Key insight: sites whose search URL uses a generic query-param endpoint
    (/search?q=, /s?k=, etc.) are large retailers / marketplaces whose
    product URLs require opaque IDs (ASIN, item ID, SKU) discovered only
    from search results. There's no way to guess a product URL from a name.
    
    Sites that DO support slug-based product URLs always expose this in
    their search URL template (/products?q=, /collections/, /shop/) or
    their product links (/products/product-name).
    """
    handle = re.sub(r"[^a-z0-9]+", "-", product_name.lower()).strip("-")

    # If the search URL template itself contains /products/, the site
    # uses slug-based product URLs → fallback is safe
    if "/products/" in search_url_template:
        return f"{base_url}/products/{handle}"

    if "/shop/" in search_url_template:
        return f"{base_url}/shop/{handle}"

    # Generic query-param search endpoints → marketplace / large retailer.
    # Product URLs use opaque IDs — no guessable fallback from product name.
    # This covers: /search?q=, /s?k=, /search?keyword=, /catalogsearch/result/
    # and any variation thereof.
    search_path = urllib.parse.urlparse(search_url_template).path.rstrip("/").lower()
    generic_search_paths = {
        "/search", "/s", "/catalogsearch/result",
        "/search/result", "/search/all",
    }
    if search_path in generic_search_paths:
        return None

    # Path-based search (e.g. /search-medicines/{query}, /search/{query}, /sch/)
    # These also can't guess a product URL from the name
    if "/search" in search_path or "/sch" in search_path:
        return None

    # Anything else — try /products/ as a reasonable guess
    # This covers custom DTC sites, Shopify variants, etc.
    return f"{base_url}/products/{handle}"


# ── OpenAI validation ─────────────────────────────────────────────────

def _validate_product_match(product_title: str, user_query: str) -> bool:
    if not _openai():
        return _heuristic_match(product_title, user_query)

    prompt = f"""
User searched for: "{user_query}"
Product found on website: "{product_title}"

Decide if the found product is the same one the user searched for.
Answer ONLY "yes" or "no".

STEP 1 — Identify distinctive words in the user's query.
Distinctive words are proper nouns, brand names, or specific non-generic terms.
Examples:
  Query "organic india ashwagandha capsules" → distinctive: "organic india" (brand)
  Query "apple iphone 17 pro 1 tb"          → distinctive: "apple", "iphone", "17 pro", "1 tb"
  Query "nivea lip balm"                    → distinctive: "nivea" (brand)
  Query "wireless earbuds"                  → distinctive: NONE (all generic)
  Query "ashwagandha capsules"              → distinctive: NONE (all generic)

STEP 2 — Apply the rules below.

Answer NO if ANY of these are true:
- The query has distinctive brand/proper-noun words that do NOT appear in the product title
- Different form (capsules vs tablets vs powder vs liquid vs effervescent)
- Different usage type (topical/external vs ingestible supplement)
- Wrong quantity if the user specified one (e.g. 60 caps vs 120 caps, 256GB vs 1TB)
- An accessory or add-on for the searched product (case, cover, charger)
- Completely unrelated product category

Answer YES if:
- All distinctive words from the query appear in the product title (word order doesn't matter)
- AND the product type/form matches
- AND the quantity (if specified) matches
- If query has NO distinctive words, any same-category product passes

Return ONLY "yes" or "no" — no explanation.
"""
    for attempt in range(2):
        try:
            response = _openai().chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=10,
            )
            answer  = response.choices[0].message.content.strip().lower()
            matched = "yes" in answer
            logger.info(
                f"{'✅ VALIDATED' if matched else '❌ REJECTED'}: '{product_title[:60]}'"
            )
            return matched

        except Exception as exc:
            err = str(exc).lower()
            if "rate_limit" in err and attempt == 0:
                logger.warning("OpenAI rate limit — retrying in 2s...")
                time.sleep(2)
                continue
            logger.warning(f"OpenAI validation failed ({exc}) — using heuristic")
            return _heuristic_match(product_title, user_query)

    return _heuristic_match(product_title, user_query)


def _heuristic_match(title: str, query: str) -> bool:
    stop = {"and", "the", "for", "with", "per", "pack", "buy", "online", "from"}
    q_words = [
        w for w in re.sub(r"[^a-z0-9 ]", "", query.lower()).split()
        if len(w) >= 4 and w not in stop
    ]
    if not q_words:
        return True
    matches  = sum(1 for w in q_words if w in title.lower())
    required = max(1, len(q_words) * 2 // 3)
    return matches >= required


# ── Product link scoring (fully generic) ──────────────────────────────

# URL path segments that signal "this is a product link, not navigation"
_PRODUCT_PATH_SIGNALS = [
    "/dp/", "/ip/", "/p/", "/products/", "/product/", "/buy/",
    "/item/", "/itm/", "-idp-", "/gp/product/", "/catalog/",
    "/shop/", "/listing/", "/offer/", "/detail/", "/sku/",
]

# URL path segments that signal "this is NOT a product link"
_NAV_PATH_SIGNALS = [
    "/stores/", "/s?", "/search", "/category/", "/categories/",
    "/brand/", "/help/", "/account/", "/login", "/signup",
    "/cart", "/checkout", "/wishlist", "/compare",
    "field-keywords", "/gp/help/", "/customer/",
    "javascript:", "mailto:", "tel:", "#",
    "/browse/brand/",
]

# URL paths that START with these are brand/category landing pages, NOT products.
# We check path.startswith() rather than substring match, because /b/ as a
# substring appears inside legitimate product URLs too (e.g. slug containing 'b').
# Examples blocked:
#   /b/organic-india/-/N-q643le90xa5    (Target brand page)
#   /b/?node=283155                     (Amazon brand hub)
_NAV_PATH_STARTSWITH = ["/b/", "/browse/"]


def _score_product_link(href: str) -> int:
    """
    Score a link href for how likely it is to be a product page link.
    Higher = more likely a product URL. Negative = definitely not a product.
    """
    if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
        return -100

    href_lower = href.lower()

    # Hard disqualifiers — substring check
    if any(p in href_lower for p in _NAV_PATH_SIGNALS):
        return -50

    # Hard disqualifiers — path prefix check.
    # Target brand pages (/b/organic-india/-/N-xxx) and Amazon brand hubs
    # (/b/?node=xxx) must be rejected without blocking legit URLs that
    # happen to contain /b/ deeper in their path.
    parsed = urllib.parse.urlparse(href)
    path_lower = parsed.path.lower()
    if any(path_lower.startswith(prefix) for prefix in _NAV_PATH_STARTSWITH):
        return -50

    score = 0

    # Product path signals
    for signal in _PRODUCT_PATH_SIGNALS:
        if signal in href_lower:
            score += 20
            break

    # Long paths with multiple segments are more likely product pages
    path = urllib.parse.urlparse(href).path
    segments = [s for s in path.split("/") if s]
    if len(segments) >= 2:
        score += 5
    if len(segments) >= 3:
        score += 5

    # Slugified paths (contains hyphens between words) look like product URLs
    if re.search(r"/[a-z0-9]+-[a-z0-9]+-[a-z0-9]+", href_lower):
        score += 10

    # Alphanumeric IDs in path (ASINs, product IDs)
    if re.search(r"/[A-Z0-9]{8,12}(?:/|$|\?)", href):
        score += 15

    return score


def _best_product_link(links: list, card) -> Optional[object]:
    """
    From all links in a card, pick the one most likely to be the product link.
    Falls back to heading links, then first link with sufficient score.
    """
    if not links:
        return None

    # Score all links
    scored = []
    for link in links:
        href = link.get("href", "")
        s = _score_product_link(href)
        if s >= 0:
            scored.append((s, link))

    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)

    # If we have a high-confidence product link, use it
    if scored and scored[0][0] >= 15:
        return scored[0][1]

    # Fallback: heading links (h2 > a, h3 > a) are usually the product title link
    for tag in ["h2", "h3", "h4"]:
        heading_link = card.select_one(f"{tag} a[href]")
        if heading_link:
            href = heading_link.get("href", "")
            if _score_product_link(href) >= 0:
                return heading_link

    # Last resort: best scored link, or first link
    if scored:
        return scored[0][1]

    return links[0] if links else None


# ── Product link extraction (fully generic) ───────────────────────────

def _extract_product_links(soup: BeautifulSoup, search_url: str) -> list[dict]:
    """
    Extract candidate product links from any search results page.

    Strategy order — all generic, no site-name checks:
      1. Structured data cards  — elements with data attributes signalling product items
      2. Generic title extraction from each card
      3. CSS href-pattern selectors covering all known product URL patterns
      4. data-ux card containers (Duda/DNAflux builder sites)
      5. Heading-anchored links as last resort

    Every strategy feeds into the same deduped list — no early returns that
    block fallback strategies from running.
    """
    seen_urls: set[str] = set()
    products:  list[dict] = []

    _NOISE_TEXT = {
        "add to cart", "buy now", "sponsored", "₹", "rs.", "$",
        "out of stock", "delivery", "free delivery", "rating", "star",
        "ratings", "review", "off", "deal", "save ", "coupon", "price", "offer",
    }

    def _is_bad_title(title: str) -> bool:
        t = title.lower()
        return (
            "out of 5 stars" in t or
            "ratings" in t or
            "review" in t or
            "stars" in t or
            bool(re.match(r"^\d+(\.\d+)?$", t)) or
            len(t.split()) < 3
        )

    _AD_PATTERNS = [
        "aax-", "/aclk", "googleadservices", "gclid",
        "sspa/click", "/sspa/", "sponsored",
        "/sp/track", "eventST=click",
        "/rd/click", "beacon/",
        "redirect.viglink", "go.skimresources",
    ]

    def _add(href: str, title: str) -> bool:
        if not href or len(title.strip()) < 5:
            return False
        # Skip ad/tracking URLs
        if any(p in href for p in _AD_PATTERNS):
            return False
        full_url = urllib.parse.urljoin(search_url, href)
        if full_url in seen_urls:
            return False
        if _is_bad_title(title):
            return False
        seen_urls.add(full_url)
        products.append({"url": full_url, "title": title.strip()})
        return True

    def _best_title_from_card(card) -> str:
        """
        Extract the best product title from any card element.
        Uses 6 strategies in order — fully generic, works on any site.
        """
        # Strategy A: aria-label on anchors (most reliable when present)
        _SKIP_ARIA_PREFIXES = (
            "visit the", "shop ", "see all", "brand store", "sponsored",
            "results for", "go to", "by ", "from ", "check out", "explore ", "browse ",
        )
        for a in card.find_all("a", attrs={"aria-label": True}):
            aria = a.get("aria-label", "").strip()
            if re.search(r"\d+(\.\d+)?\s+out\s+of\s+\d+\s+star", aria, re.IGNORECASE):
                continue
            if aria.lower().startswith(_SKIP_ARIA_PREFIXES):
                continue
            a_href = a.get("href", "")
            if "/stores/" in a_href or "/s?" in a_href or "field-keywords" in a_href:
                continue
            if len(aria.split()) >= 3:
                return aria

        # Strategy A.5: Combine brand heading + product heading
        # Myntra uses: <h3 class="product-brand">Casio</h3>
        #              <h4 class="product-product">Vintage Gold Watch...</h4>
        # Flipkart uses similar split-heading patterns.
        # Generic: if card has h3 + h4 (or h4 + h4), combine them.
        brand_selectors = [
            "[class*='brand' i]",
            "[class*='Brand' i]",
        ]
        product_selectors = [
            "[class*='product-product' i]",  # Myntra specific class
            "[class*='product-name' i]",
            "[class*='productName' i]",
            "[class*='product-title' i]",
        ]
        for bs in brand_selectors:
            brand_el = card.select_one(bs)
            if brand_el:
                brand_text = brand_el.get_text(strip=True)
                for ps in product_selectors:
                    prod_el = card.select_one(ps)
                    if prod_el:
                        prod_text = prod_el.get_text(strip=True)
                        combined = f"{brand_text} {prod_text}".strip()
                        if len(combined.split()) >= 2:
                            return combined
                # If brand found but no product selector, try next heading
                next_heading = brand_el.find_next_sibling(["h1", "h2", "h3", "h4", "h5"])
                if next_heading:
                    prod_text = next_heading.get_text(strip=True)
                    combined = f"{brand_text} {prod_text}".strip()
                    if len(combined.split()) >= 2:
                        return combined

        # Strategy B: heading tags — try combining multiple headings first
        headings = card.find_all(["h1", "h2", "h3", "h4"])
        if len(headings) >= 2:
            # Combine first two headings (common pattern: brand + product name)
            combined = " ".join(h.get_text(strip=True) for h in headings[:2])
            if len(combined.split()) >= 2:
                return combined
        # Single heading
        for el in headings:
            t = el.get_text(separator=" ", strip=True)
            if len(t.split()) >= 2:  # Relaxed from 3 to 2
                return t

        # Strategy C: common title class patterns (works across most e-commerce platforms)
        for selector in [
            "[class*='title' i]",
            "[class*='name' i]",
            "[class*='product-title' i]",
            "[class*='productName' i]",
            "[itemprop='name']",
            "[data-automation-id='name']",
        ]:
            el = card.select_one(selector)
            if el:
                t = el.get_text(separator=" ", strip=True)
                if 2 <= len(t.split()) <= 60:  # Relaxed from 3 to 2
                    return t

        # Strategy D: longest meaningful span (filters noise)
        best_span = ""
        for span in card.find_all("span"):
            st = span.get_text(strip=True)
            if any(n in st.lower() for n in _NOISE_TEXT):
                continue
            if re.search(r"^\d+\.\d+", st) or re.search(r"^\(\d", st):
                continue
            if 3 <= len(st.split()) <= 60 and len(st) > len(best_span):
                best_span = st
        if best_span:
            return best_span

        # Strategy E: link text last resort
        link = card.find("a", href=True)
        if link:
            return link.get_text(separator=" ", strip=True)

        return ""

    # ── Step 1: Generic structured product cards ──────────────────────
    # These selectors catch product cards across ALL major e-commerce platforms
    # without naming any site — they all use data attributes on their item containers.
    CARD_SELECTORS = [
        # Data-attribute based (Amazon, Walmart, eBay, generic)
        "div[data-component-type='s-search-result']",
        "li[data-asin]:not([data-asin=''])",
        "div[data-asin]:not([data-asin=''])",
        "[data-item-id]",
        "[data-automation-id='product']",
        "[data-testid*='item']",
        "[data-id]",
        # Class-pattern based (Flipkart, Nykaa, Myntra, generic)
        "[class*='search-result' i]",
        "[class*='product-item' i]",
        "[class*='productCard' i]",
        "[class*='product-card' i]",
        "[class*='product-list__item' i]",
        "[class*='Grid-col' i]",
        "li.product-base",
        # Shopify / DTC
        "[class*='product-grid-item' i]",
        "[class*='grid__item' i]",
    ]

    for selector in CARD_SELECTORS:
        cards = soup.select(selector)
        if not cards:
            continue

        logger.info(f"Found {len(cards)} cards via '{selector}'")

        for card in cards:
            # ── Skip sponsored/ad cards before doing any work ──────────
            # Sites mark sponsored listings with structural indicators:
            #   - <span>Sponsored</span> badge inside the card
            #   - aria-label="Sponsored Ad" on the card or an inner element
            #   - data-component-type containing "sp-" (Amazon ad prefix)
            #   - data-automation-id="sponsored-indicator" (Walmart)
            # We check the card's top-level attributes and a small portion
            # of inner text. This is a generic check — no site-name logic.
            card_attrs_text = " ".join(
                f"{k}={v}" for k, v in card.attrs.items()
                if isinstance(v, str)
            ).lower()
            is_sponsored = (
                "sponsored" in card_attrs_text or
                "ad-feedback" in card_attrs_text or
                "sp-" in card.get("data-component-type", "").lower()
            )
            if not is_sponsored:
                # Check a small inner badge (not full card text, which may
                # legitimately mention "sponsored by" inside a description).
                for badge in card.select(
                    "span, div[class*='sponsor' i], div[class*='ad-badge' i], "
                    "[aria-label*='Sponsored' i], [data-automation-id*='sponsor' i]"
                )[:20]:  # only check first 20 nested spans — enough to spot a badge
                    badge_text = badge.get_text(strip=True).lower()
                    if badge_text in {"sponsored", "ad", "sponsored ad", "ads"}:
                        is_sponsored = True
                        break
            if is_sponsored:
                continue

            # Find the primary link — DYNAMIC: score all links in the card
            # and pick the one that looks most like a product URL.
            # No site-specific href patterns.
            card_links = card.find_all("a", href=True)
            link_el = _best_product_link(card_links, card)
            if not link_el:
                continue

            href = link_el.get("href", "")

            # Skip ad/tracking URLs
            if any(p in href for p in [
                "aax-", "/aclk", "googleadservices", "gclid",
                "sspa/click", "/sspa/", "sponsored",
                "/sp/track", "eventST=click",        # Walmart sponsored ads
                "/rd/click", "beacon/",               # Generic ad beacons
                "redirect.viglink", "go.skimresources", # Affiliate redirects
            ]):
                continue

            # Normalise Amazon /dp/ URLs to clean canonical form
            full_url = urllib.parse.urljoin(search_url, href)
            dp_match = re.search(r"(https?://[^/]+/[^/]+/dp/[A-Z0-9]+)", full_url)
            if dp_match:
                full_url = dp_match.group(1)

            title = _best_title_from_card(card)

            if title and len(title.split()) >= 3 and full_url not in seen_urls:
                seen_urls.add(full_url)
                products.append({"url": full_url, "title": title.strip()})

            if len(products) >= 30:
                break

        if len(products) >= 5:
            # Found enough from structured cards — skip remaining card selectors
            # but still fall through to href selectors as a supplement
            break

    # ── Step 2: CSS href-pattern selectors ────────────────────────────
    # Covers any site whose product URLs follow common patterns
    HREF_SELECTORS = [
        "a[href*='/products/']",
        "a[href*='/product/']",
        "a[href*='/shop/ols/products/']",
        "a[href*='/dp/']",
        "a[href*='/gp/product/']",
        "a[href*='/ip/']",
        "a[href*='/p/itm/']",
        "a[href*='/p/']",
        "a[href*='/buy/']",
        "a[href*='-idp-']",
        "a[href*='/item/']",
        "a[href*='product']",
        "a[class*='product' i]",
        ".product-link a",
        ".product-title a",
        "li.product-base a",
        "h2 a",
        "h3 a",
        "h4 a",
    ]

    if len(products) < 15:
        for selector in HREF_SELECTORS:
            for link in soup.select(selector):
                href      = link.get("href", "")
                link_text = link.get_text(strip=True)

                if len(link_text) < 5:
                    parent = link.find_parent(["h2", "h3", "h4"])
                    if parent:
                        link_text = parent.get_text(strip=True)

                if link_text.lower() in {"buy", "view", "details", "shop", "more", "read more"}:
                    continue

                _add(href, link_text)

            if len(products) >= 15:
                break

    # ── Step 3: data-ux containers (Duda / DNAflux site builder) ─────
    if len(products) < 5:
        for card in soup.find_all(
            attrs={"data-ux": re.compile(r"Card|Product|Item", re.IGNORECASE)}
        ):
            link = card.find("a", href=True)
            if not link:
                continue
            heading = card.find(["h1", "h2", "h3", "h4"])
            title   = (
                heading.get_text(strip=True) if heading
                else link.get_text(strip=True)
            )
            _add(link.get("href", ""), title)
            if len(products) >= 15:
                break

    logger.info(f"Extracted {len(products)} candidate product links")
    return products


# ── Product validation ────────────────────────────────────────────────

def _matches_query(title: str, query_words: list[str]) -> bool:
    t       = re.sub(r"[^a-z0-9 ]", " ", title.lower())
    matches = sum(1 for w in query_words if w in t)
    if len(query_words) <= 2:
        return matches >= len(query_words)
    return matches >= 2


def find_and_validate_product(products: list, query: str) -> Optional[dict]:
    """
    Find the best matching product from a list of candidates.
    """
    stop = {"and", "the", "for", "with", "per", "pack", "buy", "online", "from", "of", "in"}
    query_words = [w for w in query.lower().split() if w not in stop]
    
    if not query_words:
        query_words = query.lower().split()
    
    # Debug: log first 5 extracted titles so we can see what cards produced
    if products:
        logger.info(f"[validate] Query: '{query}' | words: {query_words}")
        for i, p in enumerate(products[:5]):
            logger.info(f"[validate]   #{i+1}: '{p['title'][:80]}' → {p['url'][:60]}")
    
    # Validate only the top 7 results.
    # Rationale: the site's search engine already ranks by relevance. If the
    # correct product isn't in the top 7, it's almost never further down —
    # positions 8-15 are typically the site's "you might also like" fallbacks
    # which have a high rate of false positives. Scanning fewer candidates
    # produces cleaner misses and fewer wrong-brand matches.
    for product in products[:7]:
        title = product["title"]
        title_lower = title.lower()
        matches = sum(1 for w in query_words if w in title_lower)

        if len(query_words) <= 2:
            required = len(query_words)
        else:
            required = max(1, len(query_words) // 3)

        if matches < required:
            logger.debug(f"[validate] SKIP '{title[:50]}' — {matches}/{required} words matched")
            continue

        logger.info(f"[validate] CHECKING '{title[:60]}' — {matches}/{len(query_words)} words matched")
        if _validate_product_match(title, query):
            return product

    return None


# ── HTTP helpers ──────────────────────────────────────────────────────

def _headers(url: str = "", referer: str = "") -> dict:
    """
    Build request headers. Locale derived from URL TLD — not hardcoded per site.
    """
    _, _, accept_lang = _locale_for_url(url)
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": accept_lang,
        "Referer":         referer or "https://www.google.com/",
        "Connection":      "keep-alive",
    }


def _build_soup_and_text(html: str) -> tuple[BeautifulSoup, str]:
    """
    Parse HTML and build prioritised text.
    Priority selectors are all generic — no site-specific IDs except Amazon's
    well-known stable IDs which are structural, not cosmetic.
    """
    soup = BeautifulSoup(html, "lxml")

    priority_selectors = [
        # Structured product detail sections (generic + well-known stable IDs)
        "[id*='productDetails' i]",
        "[id*='feature-bullets' i]",
        "[id*='important-information' i]",
        "[data-testid*='product-description' i]",
        "[data-testid*='specifications' i]",
        # Generic class-based
        "[class*='product-detail' i]",
        "[class*='productDetail' i]",
        "[class*='item-detail' i]",
        "[class*='product-info' i]",
        "[class*='product-description' i]",
        "[class*='product-spec' i]",
    ]

    priority_parts = []
    for sel in priority_selectors:
        section = soup.select_one(sel)
        if section:
            priority_parts.append(section.get_text(separator=" ", strip=True))

    rest_text = soup.get_text(separator=" ", strip=True)
    full_text = (" ".join(priority_parts) + " " + rest_text)[:MAX_PAGE_CHARS]
    return soup, full_text


_sessions_cache: dict[str, requests.Session] = {}
_sessions_lock = threading.Lock()


def _get_session(url: str, referer: str = "") -> requests.Session:
    """
    Get or create a persistent requests.Session for this domain.
    
    Previous code: created a new Session() on every _get_soup call.
    Problem: Cookies set by the search page (CSRF tokens, session IDs,
    geo-preference cookies) were thrown away. When the product page was
    fetched with a fresh session, sites like Flipkart and Nykaa returned
    empty shells or redirected to the homepage.
    
    New code: One session per domain, reused across search → product page.
    """
    host = urllib.parse.urlparse(url).netloc.lower()
    with _sessions_lock:
        if host not in _sessions_cache:
            session = requests.Session()
            session.headers.update(_headers(url, referer))
            _sessions_cache[host] = session
        else:
            # Update referer for the existing session
            if referer:
                _sessions_cache[host].headers["Referer"] = referer
        return _sessions_cache[host]


def _get_soup(url: str, referer: str = "") -> tuple[Optional[BeautifulSoup], str]:
    """Fetch with requests. Returns (soup, full_text) or (None, '')."""
    try:
        session = _get_session(url, referer)
        response = session.get(url, timeout=TIMEOUT_REQ, allow_redirects=True)

        if response.status_code != 200:
            logger.debug(f"[req] HTTP {response.status_code} — {url[:60]}")
            return None, ""

        encoding = response.encoding or "utf-8"
        try:
            html = response.content.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            html = response.content.decode("utf-8", errors="replace")

        return _build_soup_and_text(html)

    except Exception as exc:
        logger.debug(f"[req] Error fetching {url[:60]}: {exc}")
        return None, ""


def _get_soup_scraperapi(url: str) -> tuple[Optional[BeautifulSoup], str]:
    """
    Fetch via ScraperAPI — bypasses sites with heavy bot detection.
    Only called when SCRAPER_API_KEY is set and Playwright is blocked.
    render=true enables JS rendering. country_code derived from URL TLD.
    """
    if not SCRAPER_API_KEY or SCRAPER_API_KEY.strip() == "":
        logger.warning("[scraperapi] No SCRAPER_API_KEY set — skipping. "
                       "Get a free key at https://www.scraperapi.com/")
        return None, ""

    # Detect country from URL for accurate geo-rendering
    host = urllib.parse.urlparse(url).netloc.lower()
    country = "us"
    if host.endswith(".in"):
        country = "in"
    elif host.endswith(".co.uk"):
        country = "gb"
    elif host.endswith(".com.au"):
        country = "au"
    elif host.endswith(".ca"):
        country = "ca"

    proxy_url = (
        f"http://api.scraperapi.com"
        f"?api_key={SCRAPER_API_KEY.strip()}"
        f"&url={urllib.parse.quote(url, safe='')}"
        f"&render=true"
        f"&country_code={country}"
    )

    try:
        max_retries = 2
        for attempt in range(max_retries):
            logger.info(f"[scraperapi] Fetching{' (retry)' if attempt > 0 else ''}: {url[:80]}")
            try:
                response = requests.get(proxy_url, timeout=TIMEOUT_SCRAPER)
            except requests.exceptions.Timeout:
                logger.warning(f"[scraperapi] Timeout after {TIMEOUT_SCRAPER}s (attempt {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None, ""

            if response.status_code == 200:
                logger.info(f"[scraperapi] ✅ Success")
                return _build_soup_and_text(response.text)
            elif response.status_code == 401:
                logger.error(
                    f"[scraperapi] ❌ HTTP 401 — API key is invalid or expired. "
                    f"Check SCRAPER_API_KEY in your .env file. "
                    f"Key starts with: '{SCRAPER_API_KEY[:8]}...'"
                )
                return None, ""
            elif response.status_code == 403:
                logger.warning(f"[scraperapi] HTTP 403 — site may require premium ScraperAPI plan")
                return None, ""
            elif response.status_code >= 500:
                logger.warning(f"[scraperapi] HTTP {response.status_code} (attempt {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None, ""
            else:
                logger.warning(f"[scraperapi] HTTP {response.status_code}")
                return None, ""

        return None, ""
    except Exception as exc:
        logger.warning(f"[scraperapi] Failed: {exc}")
        return None, ""


def _get_soup_playwright(url: str) -> tuple[Optional[BeautifulSoup], str, str]:
    """
    Fetch with Playwright. Returns (soup, full_text, final_url).
    Runs in its own thread+event loop for Windows ProactorEventLoop compatibility.
    """
    result: dict = {}

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_playwright_fetch(url, result))
        except Exception as exc:
            logger.warning(f"[pw] Thread error: {exc}")
        finally:
            loop.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=30)

    html      = result.get("html", "")
    final_url = result.get("url", url)

    if not html:
        return None, "", final_url

    soup, text = _build_soup_and_text(html)
    return soup, text, final_url


async def _playwright_fetch(url: str, result: dict) -> None:
    """
    Core Playwright fetch logic.
    Locale/timezone/language derived from URL TLD — no per-site if/else.
    HTTP/2 disabled only for domains known to have protocol issues.
    Wait selectors are a single generic list that works across all sites.
    """
    from playwright.async_api import async_playwright

    try:
        from playwright_stealth import stealth_async
        has_stealth = True
    except ImportError:
        has_stealth = False

    locale, timezone, accept_lang = _locale_for_url(url)
    disable_http2 = _needs_http2_disabled(url)

    async with async_playwright() as pw:
        launch_args = [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1920,1080",
            "--disable-features=IsolateOrigins,site-per-process",
        ]
        if disable_http2:
            launch_args.append("--disable-http2")

        # Serialize launches (up to 2 concurrent) to avoid Windows spawn
        # races when multiple sites scrape in parallel. Lock is released
        # as soon as the browser process exists — navigation and
        # extraction run fully in parallel after that point.
        with _playwright_launch_semaphore:
            browser = await pw.chromium.launch(headless=True, args=launch_args)

        # Use a current Chrome version — outdated versions are a bot signal
        CHROME_VERSION = "124.0.0.0"

        context = await browser.new_context(
            user_agent=(
                f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/{CHROME_VERSION} Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
            locale=locale,
            timezone_id=timezone,
            extra_http_headers={
                "Accept-Language":           accept_lang,
                "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding":           "gzip, deflate, br",
                "sec-ch-ua":                 f'"Chromium";v="124", "Not(A:Brand";v="24", "Google Chrome";v="124"',
                "sec-ch-ua-mobile":          "?0",
                "sec-ch-ua-platform":        '"Windows"',
                "Sec-Fetch-Dest":            "document",
                "Sec-Fetch-Mode":            "navigate",
                "Sec-Fetch-Site":            "none",
                "Upgrade-Insecure-Requests": "1",
            },
        )

        await context.add_init_script("""
            // Hide webdriver flag
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

            // Fake plugins array (real browsers have plugins)
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const plugins = [
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                        { name: 'Native Client', filename: 'internal-nacl-plugin' },
                    ];
                    plugins.length = 3;
                    return plugins;
                }
            });

            // Fake languages
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

            // Chrome runtime object
            window.chrome = {
                runtime: {
                    onMessage: { addListener: () => {}, removeListener: () => {} },
                    sendMessage: () => {},
                },
                loadTimes: () => ({}),
                csi: () => ({}),
            };

            // Permissions API
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (params) => (
                params.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(params)
            );

            // WebGL vendor masking
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(param) {
                if (param === 37445) return 'Intel Inc.';
                if (param === 37446) return 'Intel Iris OpenGL Engine';
                return getParameter.call(this, param);
            };

            // Prevent iframe contentWindow detection
            Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
                get: function() { return window; }
            });
        """)

        page = await context.new_page()
        if has_stealth:
            await stealth_async(page)

        # Block only fonts and large media — NOT images.
        #
        # Previous code: blocked all png/jpg/webp/svg/gif.
        # Problem: Modern SPAs (Myntra, Nykaa, Flipkart) use IntersectionObserver
        # on <img> elements to trigger product card rendering. When images are
        # aborted, the observer callback never fires, the product grid never
        # populates, and Playwright finds 0 product cards.
        #
        # New code: Block only fonts (woff/woff2/ttf) and video — these are
        # genuinely heavy assets that don't affect product card rendering.
        await page.route(
            "**/*.{woff,woff2,ttf,eot,mp4,webm,ogg}",
            lambda r: r.abort()
        )

        try:
            await page.goto(url, timeout=20000, wait_until="domcontentloaded")

            # ── Wait for content to render ────────────────────────────
            PRODUCT_SELECTORS = [
                "div[data-component-type='s-search-result']",
                "[data-item-id]",
                "[data-automation-id='product']",
                "[class*='productCard']",
                "[class*='ProductCard']",
                "[class*='product-card']",
                "[class*='product-item']",
                "[class*='product-list']",
                "li.product-base",
            ]

            # Use page.evaluate to race all selectors at once in the browser
            # — this is a single round-trip, not N sequential waits
            content_loaded = False
            for attempt in range(3):  # Check up to 3 times over ~3s
                matched_sel = await page.evaluate("""
                    (selectors) => {
                        for (const sel of selectors) {
                            try {
                                const count = document.querySelectorAll(sel).length;
                                if (count >= 2) return { sel, count };
                            } catch(e) {}
                        }
                        return null;
                    }
                """, PRODUCT_SELECTORS)

                if matched_sel:
                    logger.info(f"[pw] ✅ '{matched_sel['sel']}' → {matched_sel['count']} elements")
                    content_loaded = True
                    break

                await page.wait_for_timeout(1000)

            if not content_loaded:
                logger.info("[pw] No product selectors matched — waiting for network idle")
                try:
                    await page.wait_for_load_state("networkidle", timeout=3000)
                except Exception:
                    pass

            # Quick scroll to trigger lazy loading
            await page.mouse.wheel(0, random.randint(600, 1000))
            await page.wait_for_timeout(300)

            html = await page.content()

            # ── Bot detection check ──────────────────────────────────────
            # IMPORTANT: Check the VISIBLE PAGE TEXT, not raw HTML.
            # Raw HTML contains JS bundles, tracking scripts, and error
            # handling code that routinely include words like "captcha",
            # "blocked", "access denied" even on successfully loaded pages.
            visible_text = ""
            try:
                visible_text = await page.evaluate("() => document.body?.innerText || ''")
            except Exception:
                pass
            visible_lower = visible_text.lower()

            # Exact phrases that ONLY appear on actual block pages
            # — never in normal product page text
            block_phrases = [
                "please verify you are a human",
                "verify you are human",
                "are you a human",
                "robot check",
                "enter the characters you see below",
                "automated access to this page",
                "press & hold to confirm you are a human",
                "press and hold to confirm",
                "bot or not",
                "sorry, we just need to make sure you're not a robot",
                "please enable cookies",
                "enable javascript to continue",
                "access to this page has been denied",
                "this request was blocked by our security service",
            ]

            is_blocked = any(phrase in visible_lower for phrase in block_phrases)

            # Secondary check: if the page is very short AND has a
            # suspicious title, it's likely a challenge page
            if not is_blocked and len(visible_text.strip()) < 200:
                try:
                    title = await page.title()
                    title_lower = title.lower()
                    challenge_titles = [
                        "robot check", "access denied", "security check",
                        "just a moment", "attention required",
                        "please wait", "verification",
                    ]
                    if any(ct in title_lower for ct in challenge_titles):
                        is_blocked = True
                except Exception:
                    pass

            if is_blocked:
                logger.warning(f"[pw] ❌ Bot detection / block on {url[:80]}")
                result["html"] = ""
                result["url"]  = url
                await browser.close()
                return

            result["html"] = html
            result["url"]  = page.url

        except Exception as exc:
            err = str(exc).lower()
            if "err_http2_protocol_error" in err:
                logger.warning("[pw] HTTP/2 error, retrying without HTTP/2...")
                try:
                    await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                    result["html"] = await page.content()
                    result["url"]  = page.url
                except Exception:
                    result["html"] = ""
                    result["url"]  = url
            elif any(e in err for e in ["net::", "timeout", "err_connection"]):
                logger.warning(f"[pw] Network error: {exc}")
                result["html"] = ""
                result["url"]  = url
            else:
                raise
        finally:
            await browser.close()


# ── Extraction functions ──────────────────────────────────────────────

# Currency code → display symbol mapping
_CURRENCY_SYMBOLS = {
    "INR": "₹", "USD": "$", "EUR": "€", "GBP": "£",
    "JPY": "¥", "KRW": "₩", "BRL": "R$", "AUD": "A$",
    "CAD": "C$", "SGD": "S$", "AED": "AED ", "BDT": "৳",
    "NPR": "Rs", "PKR": "Rs", "LKR": "Rs",
}


def _symbol_for_currency(currency_code: str) -> str:
    """Convert currency code to display symbol. Falls back to code itself."""
    return _CURRENCY_SYMBOLS.get(currency_code.upper(), currency_code + " ")

def extract_price_universal(
    soup: BeautifulSoup,
    full_text: str,
    page_url: str = "",
) -> tuple[float, str]:
    """
    Extract product price AND currency from the page HTML.
    
    Returns (price, currency_code) e.g. (599.0, "INR") or (29.99, "USD").
    
    Currency detection: reads the ACTUAL symbol from the page, not the URL.
    Previous code: guessed from URL TLD — .com → USD, .in → INR.
    Problem: A .com site selling in EUR would show "$". A European Shopify
    store on .com would show "$" instead of "€".
    
    New code: Detects currency from:
      1. JSON-LD priceCurrency field (most reliable)
      2. og:price:currency meta tag
      3. The symbol attached to the price (₹, $, €, £, ¥)
      4. TLD-based fallback ONLY if nothing found on page
    """
    # We'll track what currency we find alongside the price
    detected_currency = None

    # ── Helper: infer min_price floor from currency ───────────────────
    def _min_price_for(currency: str) -> float:
        """INR prices are 80+, USD/EUR/GBP are 0.5+"""
        if currency in ("INR", "BDT", "NPR", "LKR", "PKR"):
            return 80.0
        if currency in ("JPY", "KRW"):
            return 50.0
        return 0.5

    # ── Helper: detect currency from symbol ──────────────────────────
    def _currency_from_symbol(text: str) -> str:
        """Look at the text surrounding a price to detect currency symbol."""
        text = text.strip()
        if "₹" in text:
            return "INR"
        # Rs / Rs. / Rs.  / INR at the start or near a number
        if re.search(r"\bRs\.?\s", text) or re.search(r"\bINR\b", text):
            return "INR"
        if "€" in text:
            return "EUR"
        if "£" in text:
            return "GBP"
        if "¥" in text:
            return "JPY"
        if "₩" in text:
            return "KRW"
        if "R$" in text:
            return "BRL"
        if "$" in text:
            return "USD"
        return ""

    # Fallback: TLD-based guess (only used if page gives zero currency signals)
    def _tld_currency_fallback() -> str:
        parsed = urllib.parse.urlparse(page_url)
        host = parsed.netloc.lower()
        if host.endswith(".in") or host.endswith(".co.in"):
            return "INR"
        if host.endswith(".co.uk"):
            return "GBP"
        if host.endswith(".de") or host.endswith(".fr") or host.endswith(".it") or host.endswith(".es"):
            return "EUR"
        if host.endswith(".jp"):
            return "JPY"
        if host.endswith(".com.au"):
            return "AUD"
        if host.endswith(".ca"):
            return "CAD"
        # For .com, check if it's a known Indian domain
        if host.endswith(".com") or host.endswith(".co"):
            domain_parts = _extract_all_domain_parts(host)
            if domain_parts & _INDIAN_DOMAIN_SEEDS:
                return "INR"
        return "USD"

    # Strategy 1: JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data   = json.loads(script.string or "")
            offers = data.get("offers") if isinstance(data, dict) else None
            if not offers:
                continue
            offer = offers[0] if isinstance(offers, list) else offers
            price_val = offer.get("price", 0)
            # JSON-LD has explicit priceCurrency — most reliable source
            ld_currency = offer.get("priceCurrency", "")
            if price_val:
                price = float(str(price_val).replace(",", ""))
                if ld_currency:
                    detected_currency = ld_currency.upper()
                min_p = _min_price_for(detected_currency or _tld_currency_fallback())
                if price >= min_p:
                    if not detected_currency:
                        detected_currency = _tld_currency_fallback()
                    logger.info(f"💰 JSON-LD price: {price} {detected_currency}")
                    return price, detected_currency
        except Exception:
            pass

    # Strategy 2: Open Graph / meta tags
    for meta_name in ["product:price:amount", "og:price:amount"]:
        tag = (
            soup.find("meta", property=meta_name) or
            soup.find("meta", attrs={"name": meta_name})
        )
        if tag and tag.get("content"):
            try:
                price = float(re.sub(r"[^\d.]", "", tag["content"].replace(",", "")))
                # Check for og:price:currency
                curr_tag = (
                    soup.find("meta", property="product:price:currency") or
                    soup.find("meta", property="og:price:currency") or
                    soup.find("meta", attrs={"name": "product:price:currency"})
                )
                if curr_tag and curr_tag.get("content"):
                    detected_currency = curr_tag["content"].upper()
                min_p = _min_price_for(detected_currency or _tld_currency_fallback())
                if price >= min_p:
                    if not detected_currency:
                        detected_currency = _tld_currency_fallback()
                    logger.info(f"💰 Meta tag price: {price} {detected_currency}")
                    return price, detected_currency
            except Exception:
                pass

    # Strategy 2b: Meta description price (catches Myntra's "Rs. 110" in meta)
    meta_desc = (
        soup.find("meta", property="og:description") or
        soup.find("meta", attrs={"name": "description"})
    )
    if meta_desc and meta_desc.get("content"):
        desc_text = meta_desc["content"]
        price_match = re.search(r"(₹|Rs\.?|EUR|€|\$|£|¥)\s*(\d+(?:,\d+)*(?:\.\d+)?)", desc_text)
        if price_match:
            try:
                symbol_text = price_match.group(1)
                price = float(price_match.group(2).replace(",", ""))
                detected_currency = _currency_from_symbol(symbol_text)
                min_p = _min_price_for(detected_currency or _tld_currency_fallback())
                if price >= min_p:
                    if not detected_currency:
                        detected_currency = _tld_currency_fallback()
                    logger.info(f"💰 Meta description price: {price} {detected_currency}")
                    return price, detected_currency
            except Exception:
                pass

    # Strategy 3: CSS selectors
    CSS_SELECTORS = [
        "span[itemprop='price']",
        "[itemprop='price']",
        ".current-price .price",
        "#product-price-display",
        ".price-item--sale",
        ".price-item--regular",
        ".product__price",
        "._30jeq3",
        "[data-testid='price-wrap']",
        "[data-testid*='price' i]",
        ".product-price",
        ".current-price",
        ".sale-price",
        ".final-price",
        "[data-price]",
        ".product-price-current",
        ".js-product-price",
        ".rupee",
        ".price",
    ]

    # Amazon-specific: .a-offscreen has clean prices like "$1,249.97" BUT there are
    # many .a-offscreen spans on the page (list price, savings, EMI amounts, accessories).
    # .a-price-whole drops the decimal ($7.94 → 794). .a-offscreen is correct but
    # we need to pick the RIGHT one.
    #
    # Amazon marks the actual sale price with these attributes:
    #   <span class="a-price apexPriceToPay" data-a-color="price">
    #       <span class="a-offscreen">$1,249.97</span>
    #   </span>
    #
    # Strategy: 3 attempts in order of precision.

    # Attempt A: precise Amazon price container
    #
    # Amazon updates their class names periodically. As of April 2026 the
    # real buy-box uses data-a-color="base" (not "price") and priceToPay
    # (not apexPriceToPay) with apex-pricetopay-value nested inside.
    # We check NEW selectors first (most specific), then OLD as fallback
    # in case Amazon A/B tests layouts between users.
    for price_sel in [
        # NEW — Amazon's current buy-box selectors (seen in April 2026)
        '[data-a-color="base"].priceToPay .a-offscreen',
        '.priceToPay .a-offscreen',
        '.apex-pricetopay-value .a-offscreen',
        '.reinventPricePriceToPayMargin .a-offscreen',
        '.apex-core-price-identifier .a-offscreen',
        # OLD — kept in case Amazon still serves these to some sessions
        '[data-a-color="price"] .a-offscreen',
        '.apexPriceToPay .a-offscreen',
        # Container-based — these IDs are stable across Amazon layouts
        '#corePrice_feature_div .a-offscreen',
        '#corePriceDisplay_desktop_feature_div .a-offscreen',
    ]:
        price_container = soup.select_one(price_sel)
        if price_container:
            raw_text = price_container.get_text().strip()
            curr = _currency_from_symbol(raw_text)
            price_match = re.search(r"[\d,]+\.?\d*", raw_text)
            if curr and price_match:
                try:
                    price = float(price_match.group(0).replace(",", ""))
                    min_p = _min_price_for(curr)
                    if price >= min_p:
                        logger.info(f"💰 Amazon price ({price_sel.split()[0]}): {price} {curr}")
                        return price, curr
                except ValueError:
                    pass

    # Attempt B: scan ALL .a-offscreen, pick HIGHEST-RECURRING price.
    #
    # Why not mode (most-frequent)? On iPhone/expensive product pages,
    # Amazon shows many price references: buy-box price (2-4x), EMI per
    # month (4-6x), No-Cost-EMI deposit (4x), exchange offers, accessory
    # prices in carousels, cashback amounts. Mode often picks an EMI
    # amount instead of the real price.
    #
    # Better: pick the HIGHEST price that appears at least twice.
    # Real product price > EMI/month > cashback > accessories.
    # Appearing 2+ times rules out one-off noise like random numbers.
    a_offscreen_tags = soup.select(".a-offscreen")
    if a_offscreen_tags:
        candidates = []
        for tag in a_offscreen_tags:
            raw_text = tag.get_text().strip()
            if not raw_text:
                continue
            curr = _currency_from_symbol(raw_text)
            if not curr:
                continue
            price_match = re.search(r"[\d,]+\.\d{2}", raw_text)
            if not price_match:
                price_match = re.search(r"[\d,]+", raw_text)
            if not price_match:
                continue
            try:
                price = float(price_match.group(0).replace(",", ""))
                min_p = _min_price_for(curr)
                if price >= min_p:
                    candidates.append((price, curr))
            except (ValueError, IndexError):
                continue

        if candidates:
            from collections import Counter
            price_counts = Counter(candidates)

            # Primary: highest price appearing 2+ times (the real product price)
            recurring = [
                (p, c) for (p, c), count in price_counts.items() if count >= 2
            ]
            if recurring:
                recurring.sort(key=lambda x: x[0], reverse=True)
                best_price, best_curr = recurring[0]
                count = price_counts[(best_price, best_curr)]
                logger.info(
                    f"💰 Amazon .a-offscreen price: {best_price} {best_curr} "
                    f"(highest-recurring, {count}x in {len(candidates)} candidates)"
                )
                return best_price, best_curr

            # Fallback: if nothing appears twice (rare), use mode
            (best_price, best_curr), count = price_counts.most_common(1)[0]
            logger.info(
                f"💰 Amazon .a-offscreen price: {best_price} {best_curr} "
                f"(mode fallback, {count}x in {len(candidates)} candidates)"
            )
            return best_price, best_curr
    for selector in CSS_SELECTORS:
        try:
            tag = soup.select_one(selector)
            if not tag:
                continue
            content = tag.get("content") or tag.get("data-price")
            if content:
                price = float(re.sub(r"[^\d.]", "", content.replace(",", "")))
                # Try to find currency from parent/sibling text
                parent_text = tag.parent.get_text() if tag.parent else ""
                detected_currency = _currency_from_symbol(parent_text) or _currency_from_symbol(tag.get_text())
                min_p = _min_price_for(detected_currency or _tld_currency_fallback())
                if price >= min_p:
                    if not detected_currency:
                        detected_currency = _tld_currency_fallback()
                    logger.info(f"💰 CSS attr '{selector}' price: {price} {detected_currency}")
                    return price, detected_currency
            raw_text = tag.get_text()
            raw = re.sub(r"[^\d.]", "", raw_text.replace(",", ""))
            if raw:
                price = float(raw)
                detected_currency = _currency_from_symbol(raw_text)
                min_p = _min_price_for(detected_currency or _tld_currency_fallback())
                if price >= min_p:
                    if not detected_currency:
                        detected_currency = _tld_currency_fallback()
                    logger.info(f"💰 CSS '{selector}' price: {price} {detected_currency}")
                    return price, detected_currency
        except Exception:
            pass

    # Strategy 3b: data-ux selectors (Duda/DNAflux sites)
    for selector in [
        "[data-ux='CommerceItemPrice']",
        "[data-ux='CommercePrice']",
        "[data-ux='CartItemPrice']",
        "[data-aid*='PRICE']",
        "[data-aid*='Price']",
    ]:
        try:
            tag = soup.select_one(selector)
            if not tag:
                continue
            num = tag.get("number")
            if num:
                price = float(num)
                detected_currency = _currency_from_symbol(tag.get_text()) or _tld_currency_fallback()
                min_p = _min_price_for(detected_currency)
                if price >= min_p:
                    logger.info(f"💰 data-ux price: {price} {detected_currency}")
                    return price, detected_currency
            raw_text = tag.get_text()
            raw = re.sub(r"[^\d.]", "", raw_text.replace(",", ""))
            if raw:
                price = float(raw)
                detected_currency = _currency_from_symbol(raw_text) or _tld_currency_fallback()
                min_p = _min_price_for(detected_currency)
                if price >= min_p:
                    logger.info(f"💰 data-ux text price: {price} {detected_currency}")
                    return price, detected_currency
        except Exception:
            pass

    # Strategy 4: Regex on page text — detect currency FROM THE SYMBOL found
    #
    # Each pattern captures the currency symbol alongside the price.
    # This is the only reliable way to know currency on unknown sites.

    # Patterns: (currency_code, regex_with_capture_group_for_amount)
    currency_patterns = [
        ("INR", r"₹\s*(\d+(?:,\d+)*(?:\.\d+)?)"),
        ("INR", r"Rs\.?\s*(\d+(?:,\d+)*(?:\.\d+)?)"),
        ("EUR", r"€\s*(\d+(?:,\d+)*(?:\.\d+)?)"),
        ("GBP", r"£\s*(\d+(?:,\d+)*(?:\.\d+)?)"),
        ("JPY", r"¥\s*(\d+(?:,\d+)*(?:\.\d+)?)"),
        ("USD", r"\$\s*(\d+(?:,\d+)*(?:\.\d+)?)"),
    ]

    # Phase 1: Priority zone (first 5000 chars — near product title)
    # Phase 1: Priority zone (first 5000 chars — near product title)
    priority_text = full_text[:5000]
    priority_prices = []
    for curr_code, pattern in currency_patterns:
        for m in re.finditer(pattern, priority_text, re.IGNORECASE):
            try:
                p = float(m.group(1).replace(",", ""))
                min_p = _min_price_for(curr_code)
                if p >= min_p:
                    priority_prices.append((m.start(), p, curr_code))
            except Exception:
                pass

    if priority_prices:
        priority_prices.sort(key=lambda x: x[0])
        _, price, detected_currency = priority_prices[0]
        logger.info(
            f"💰 Regex price (priority zone): {price} {detected_currency} "
            f"(from {len(priority_prices)} candidates in first 5000 chars)"
        )
        return price, detected_currency

    # Phase 2: Full page scan — pick most frequently occurring price
    # Phase 2: Full page scan — pick most frequently occurring price
    all_prices = []
    for curr_code, pattern in currency_patterns:
        for m in re.findall(pattern, full_text, re.IGNORECASE):
            try:
                p = float(m.replace(",", ""))
                min_p = _min_price_for(curr_code)
                if p >= min_p:
                    all_prices.append((p, curr_code))
            except Exception:
                pass

    if all_prices:
        from collections import Counter
        price_counts = Counter(all_prices)
        (price, detected_currency) = price_counts.most_common(1)[0][0]
        logger.info(
            f"💰 Regex price (full page, mode): {price} {detected_currency} "
            f"(from {len(all_prices)} candidates)"
        )
        return price, detected_currency

    # Phase 3: Generic "price" keyword pattern (no symbol found)
    generic_match = re.search(
        r'price[:\s]*(\d+(?:,\d+)*(?:\.\d+)?)', priority_text, re.IGNORECASE
    )
    if generic_match:
        try:
            price = float(generic_match.group(1).replace(",", ""))
            detected_currency = _tld_currency_fallback()
            min_p = _min_price_for(detected_currency)
            if price >= min_p:
                logger.info(f"💰 Generic price keyword: {price} {detected_currency} (TLD fallback)")
                return price, detected_currency
        except Exception:
            pass

    logger.warning("⚠️ Price extraction failed — returning 0")
    return 0.0, _tld_currency_fallback()


def _is_food_or_supplement_page(full_text: str) -> bool:
    text_lower = full_text.lower()
    hits = sum(1 for signal in _FOOD_SIGNALS if signal in text_lower)
    return hits >= 2


def extract_supplement_facts(soup: BeautifulSoup, website_name: str = "") -> str:
    """Extract supplement facts table rows as pipe-separated string."""
    SKIP_PATTERNS = [
        r"^ingredient", r"amount per serving", r"daily value",
        r"^\*+percent", r"^\*+daily value not established",
        r"based on\s+\d+\s+calorie", r"^\*{1,3}$",
        r"^serving size", r"^servings per",
    ]

    def _should_skip(text: str) -> bool:
        t = text.strip().lower()
        return any(re.search(p, t) for p in SKIP_PATTERNS)

    def _parse_table(table) -> list[str]:
        rows_out = []
        for row in table.find_all("tr"):
            cols = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
            cols = [c for c in cols if c]
            if not cols:
                continue
            if len(cols) == 1 and not _should_skip(cols[0]):
                rows_out.append(cols[0])
            elif len(cols) == 2:
                if not _should_skip(cols[0]):
                    rows_out.append(f"{cols[0]}: {cols[1]}")
            elif len(cols) >= 3:
                dv      = cols[2].strip()
                dv_part = f" ({dv})" if dv and dv not in {"", "-", "—", "**", "*"} else ""
                if not _should_skip(cols[0]):
                    rows_out.append(f"{cols[0]}: {cols[1]}{dv_part}")
        return rows_out

    # Strategy 1: heading containing "Supplement Facts" → next table
    for heading in soup.find_all(
        ["h1", "h2", "h3", "h4", "h5", "h6", "p", "div", "span", "strong", "b"]
    ):
        if "supplement facts" in heading.get_text(strip=True).lower():
            table = heading.find_next("table")
            if table:
                parsed = _parse_table(table)
                if parsed:
                    logger.info(f"✅ Supplement facts: {len(parsed)} rows via heading")
                    return "|".join(parsed)

    # Strategy 2: Generic supplement-facts table class (works for any site using standard classes)
    for selector in [
        "table[class*='supplement-facts' i]",
        "table[class*='nutrition' i]",
        ".supplement-facts table",
        "#supplement-facts table",
    ]:
        table = soup.select_one(selector)
        if table:
            parsed = _parse_table(table)
            if parsed:
                return "|".join(parsed)

    # Strategy 3: Generic CSS containers
    for selector in [
        ".supplement-facts", "#supplement-facts",
        ".nutrition-table", ".specs-table",
    ]:
        container = soup.select_one(selector)
        if container:
            items = [
                item.get_text(strip=True)
                for item in container.find_all(["li", "tr"])
                if not _should_skip(item.get_text(strip=True))
            ]
            if items:
                return "|".join(items)

    return ""


def extract_nutrition_facts(soup: BeautifulSoup, full_text: str) -> str:
    """Extract nutritional information (Calories, Protein, etc.)."""
    nutrition: list[str] = []
    keywords = ["CALORIES", "TOTAL FAT", "PROTEIN", "CARBOHYDRATE", "SODIUM", "ENERGY"]

    for table in soup.find_all("table"):
        text = table.get_text().upper()
        if any(k in text for k in ["NUTRITION FACTS", "NUTRITIONAL INFORMATION"]):
            for row in table.find_all("tr"):
                cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cols) >= 2:
                    nutrition.append(f"{cols[0]}: {cols[1]}")

    if not nutrition:
        for k in keywords:
            m = re.search(rf"{k}[:\s]+(\d+\s?\w+)", full_text, re.IGNORECASE)
            if m:
                nutrition.append(f"{k.capitalize()}: {m.group(1)}")

    return " | ".join(nutrition[:15]) if nutrition else ""


def extract_ingredients_universal(soup: BeautifulSoup, full_text: str) -> str:
    """Universal ingredient extraction — tries 9 strategies in reliability order."""

    INVALID_PATTERNS = [
        "contains no", "contains none", "no additives", "no preservatives",
        "this product contains no", "free from", "does not contain",
        "keyboard shortcut", "shift + alt", "product summary", "skip to main",
        "buying options", "delivering to",
    ]
    VALID_STARTERS = [
        "ingredients:", "ingredient:", "key ingredients:", "key ingredient:",
        "key ingredient -", "other ingredients:", "other ingredient:",
        "other ingredients -", "ingredients -",
    ]
    INGREDIENT_SIGNALS = [
        ",", "extract", "powder", "mg", "acid", "vitamin", "mineral",
        "oil", "root", "herb", "capsule", "tablet", "blend",
    ]
    USAGE_SIGNALS = [
        "take ", "use ", "apply ", "as a dietary", "supplement, take",
        "per day", "as directed", "dissolve", "mix with", "consume",
    ]

    def _invalid(text: str) -> bool:
        return any(p in text.lower() for p in INVALID_PATTERNS)

    def _has_signal(text: str) -> bool:
        return any(s in text.lower() for s in INGREDIENT_SIGNALS)

    # P0: Amazon important-information div (stable ID)
    ii = soup.find("div", {"id": "important-information"})
    if ii:
        m = re.search(
            r"(?:Other\s+)?Ingredients?[:\s]+(.+?)(?=Directions|Warnings|Legal|$)",
            ii.get_text(separator=" ", strip=True),
            re.IGNORECASE | re.DOTALL,
        )
        if m and len(m.group(1).strip()) > 10:
            return m.group(1).strip()[:500]

    # P1: Amazon feature bullets (stable ID)
    for bullet in soup.select("#feature-bullets ul li span"):
        text = bullet.get_text(strip=True)
        if "ingredient" in text.lower() and len(text) > 10:
            return text

    # P2: Product detail table with "ingredient" key
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2 and "ingredient" in cells[0].get_text(strip=True).lower():
            value = cells[1].get_text(strip=True)
            if 10 < len(value) < 800 and not _invalid(value):
                return value

    # P3: Accordion with 'ingr' in id (Shopify / generic)
    for accordion in soup.find_all(id=re.compile(r"ingr", re.IGNORECASE)):
        text = accordion.get_text(separator=" ", strip=True)
        if any(w in text.lower() for w in ["delivering to", "update location", "pincode"]):
            continue
        if 10 < len(text) < 1000 and not _invalid(text) and _has_signal(text):
            return text

    # P4: Accordion after exact "Ingredients" heading
    for heading in soup.find_all(["h2", "h3", "h4", "summary", "button"]):
        if heading.get_text(strip=True).lower().strip() != "ingredients":
            continue
        for sibling in heading.find_all_next():
            tag_name = sibling.name or ""
            if tag_name in ["h2", "h3", "h4"] and sibling != heading:
                break
            if tag_name == "div":
                cls = " ".join(sibling.get("class", []))
                if re.search(
                    r"accordion.*content|collapsible.*content|metafield|rte",
                    cls, re.IGNORECASE
                ):
                    metafield = sibling.find(
                        "div", class_=re.compile(r"metafield", re.IGNORECASE)
                    )
                    target = metafield if metafield else sibling
                    text   = target.get_text(separator=" ", strip=True)
                    if any(s in text.lower() for s in USAGE_SIGNALS):
                        continue
                    if 10 < len(text) < 1000 and not _invalid(text):
                        return text

    # P5: Exact "Ingredients:" label in any inline tag
    for tag in soup.find_all(["p", "li", "span", "div", "td"]):
        text  = tag.get_text(strip=True)
        lower = text.lower()
        if any(lower.startswith(s) for s in VALID_STARTERS):
            parts = re.split(r"[-:]", text, maxsplit=1)
            if len(parts) > 1:
                value = parts[1].strip()
                if 10 < len(value) < 800 and not _invalid(value):
                    return value

    # P6: Metafield div (Beato / Shopify)
    for metafield in soup.find_all("div", class_=re.compile(r"metafield", re.IGNORECASE)):
        text  = metafield.get_text(separator=" ", strip=True)
        lower = text.lower()
        if any(kw in lower for kw in ["ingredient", "contains", "composition"]):
            m = re.split(
                r"(?:key\s+)?ingredients?\s*[-:]\s*",
                text, maxsplit=1, flags=re.IGNORECASE
            )
            if len(m) > 1 and 5 < len(m[1].strip()) < 800 and not _invalid(m[1]):
                return m[1].strip()

    # P7: <strong>/<b> label followed by content
    for tag in soup.find_all(["b", "strong"]):
        label = tag.get_text(strip=True).lower()
        if label not in {"ingredients:", "other ingredients:", "key ingredients:"}:
            continue
        next_node = tag.next_sibling
        if next_node and isinstance(next_node, str):
            value = next_node.strip().strip('"')
            if 10 < len(value) < 800 and not _invalid(value):
                return value
        parent = tag.parent
        if parent:
            value = parent.get_text(strip=True)
            value = value.replace(tag.get_text(strip=True), "").lstrip(": -").strip()
            if 10 < len(value) < 800 and not _invalid(value):
                return value

    # P8: Standalone label followed by sibling element
    for tag in soup.find_all(["p", "li", "span", "div", "td"]):
        if tag.get_text(strip=True).lower() in {
            "ingredients", "key ingredients", "other ingredients"
        }:
            sibling = tag.find_next_sibling()
            if sibling:
                value = sibling.get_text(strip=True)
                if 10 < len(value) < 800 and not _invalid(value):
                    return value

    # P9: Full text regex fallback
    m = re.search(
        r"(?:Other\s+)?Ingredients?[:\s]+([A-Za-z0-9\s,\(\)\.%\-\*\/]+?)"
        r"(?=\n|\.|Directions|Warnings|Customer|Also\s+bought|Price\s+₹|$)",
        full_text,
        re.IGNORECASE,
    )
    if m:
        ingr = m.group(1).strip()[:500]
        if len(ingr) > 10 and _has_signal(ingr):
            return ingr

    return ""


def extract_description(soup: BeautifulSoup, full_text: str) -> str:
    """
    Extract main product description.
    Tries JSON-LD, meta tags, feature bullets, then generic class patterns.
    """
    # Strategy 1: JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict) and data.get("description"):
                desc = data["description"].strip()
                if 20 < len(desc) < 2000:
                    return desc
        except Exception:
            pass

    # Strategy 2: og:description / meta description
    meta = (
        soup.find("meta", property="og:description") or
        soup.find("meta", attrs={"name": "description"})
    )
    if meta and meta.get("content"):
        desc = meta["content"].strip()
        if 20 < len(desc) < 1000:
            return desc

    # Strategy 3: Feature bullets (Amazon stable ID — also appears on other platforms)
    bullets_container = soup.select_one("#feature-bullets")
    if bullets_container:
        bullets = [
            li.get_text(separator=" ", strip=True)
            for li in bullets_container.select("ul li span")
            if len(li.get_text(strip=True)) > 15
        ]
        if bullets:
            return " | ".join(bullets[:6])

    # Strategy 4: Generic description containers
    for selector in [
        "[class*='product-description' i]",
        "[class*='productDescription' i]",
        "[data-testid*='product-description' i]",
        "[data-testid*='description' i]",
        ".product__description",
        ".pdp-description",
        "#productDescription",
        "[class*='highlight' i]",
        ".description",
    ]:
        container = soup.select_one(selector)
        if container:
            # Check for list items (product highlights)
            items = [li.get_text(strip=True) for li in container.find_all("li") if li.get_text(strip=True)]
            if items:
                return " | ".join(items[:6])
            text = container.get_text(separator=" ", strip=True)
            if 20 < len(text) < 3000:
                return text

    return ""


def extract_specifications(soup: BeautifulSoup, full_text: str) -> dict:
    """
    Extract structured key-value product specifications.
    Works dynamically across all sites — no per-site logic.
    """
    specs: dict[str, str] = {}

    SKIP_KEYS = {
        "click here", "see more", "learn more", "read more", "add to cart",
        "buy now", "view all", "show more", "customer reviews",
        "best sellers rank", "date first available", "feedback", "report",
    }

    def _clean_key(k: str) -> str:
        return re.sub(r"\s+", " ", k.strip().rstrip(":").lower())

    def _clean_val(v: str) -> str:
        return re.sub(r"\s+", " ", v.strip())

    def _add(key: str, val: str) -> None:
        k = _clean_key(key)
        v = _clean_val(val)
        if k and v and k not in SKIP_KEYS and len(k) < 80 and len(v) < 500:
            specs.setdefault(k, v)

    # Strategy 1: All HTML tables with ≥2 columns
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                _add(cells[0].get_text(separator=" ", strip=True),
                     cells[1].get_text(separator=" ", strip=True))

    # Strategy 2: Amazon detail bullet lists (stable IDs)
    for detail_div in soup.select(
        "#detailBullets_feature_div li, "
        "#productDetails_detailBullets_sections1 tr, "
        "#productDetails_techSpec_section_1 tr"
    ):
        cells = detail_div.find_all(["td", "th", "span"])
        if len(cells) >= 2:
            _add(cells[0].get_text(strip=True), cells[-1].get_text(strip=True))

    # Strategy 3: Definition lists
    for dl in soup.find_all("dl"):
        for k, v in zip(dl.find_all("dt"), dl.find_all("dd")):
            _add(k.get_text(strip=True), v.get_text(strip=True))

    # Strategy 4: Generic class-based key-value containers
    for selector in [
        "[class*='spec' i]",
        "[class*='detail' i]",
        "[class*='attribute' i]",
        "[class*='product-info' i]",
        "[class*='item-detail' i]",
        "[data-testid*='specifications' i]",
    ]:
        for container in soup.select(selector):
            label = container.select_one(
                "[class*='label' i], [class*='key' i], [class*='name' i], dt, th"
            )
            value = container.select_one(
                "[class*='value' i], [class*='val' i], [class*='data' i], dd, td"
            )
            if label and value:
                _add(label.get_text(strip=True), value.get_text(strip=True))

    # Strategy 5: spec list items with pipe separator (Walmart style)
    for item in soup.select("[data-testid*='specification' i] li, [class*='spec-item' i]"):
        text  = item.get_text(separator="|", strip=True)
        parts = text.split("|")
        if len(parts) >= 2:
            _add(parts[0], parts[1])

    return specs


def extract_manufacturer(
    soup: BeautifulSoup,
    full_text: str,
    product_name: str,
) -> dict:
    """
    Extract manufacturer/marketer details.
    Collects relevant text sections and sends to GPT-4o-mini for structured extraction.
    Falls back to regex if LLM unavailable.
    """
    if not _openai():
        return _regex_manufacturer_fallback(soup, full_text)

    raw_sections: list[str] = []

    for table in soup.find_all("table"):
        text = table.get_text(separator=" | ", strip=True)
        if any(kw in text.lower() for kw in [
            "manufacturer", "country", "marketer", "brand",
            "origin", "marketed", "manufactured", "packer", "importer",
        ]):
            raw_sections.append(text[:2000])

    for tag in soup.find_all(["div", "section", "ul", "p"]):
        text = tag.get_text(separator=" ", strip=True)
        if any(kw in text.lower() for kw in [
            "manufactured by", "marketed by", "country of origin",
            "manufacturer:", "marketer:", "distributed by",
            "manufactured in", "made in",
        ]) and 20 < len(text) < 2000:
            raw_sections.append(text)

    for row in soup.select(
        '[class*="detail" i], [class*="spec" i], [class*="info" i], [class*="fact" i]'
    ):
        text = row.get_text(separator=" | ", strip=True)
        if any(kw in text.lower() for kw in ["manufacturer", "country", "marketer", "origin"]):
            if len(text) < 500:
                raw_sections.append(text)

    if not raw_sections:
        raw_sections.append(full_text[:3000])

    seen, unique = set(), []
    for s in raw_sections:
        key = s[:100]
        if key not in seen:
            seen.add(key)
            unique.append(s)

    combined = "\n---\n".join(unique)[:4000]

    try:
        response = _openai().chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            max_tokens=400,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Extract manufacturer and product origin details from e-commerce product page text.\n"
                        "Return ONLY a JSON object with these exact fields:\n"
                        "{\n"
                        '  "manufacturer_name":    "company that MANUFACTURES the product",\n'
                        '  "manufacturer_address": "full address of manufacturer",\n'
                        '  "marketed_by_name":     "company explicitly labeled as Marketer",\n'
                        '  "marketed_by_address":  "address of marketer",\n'
                        '  "country_of_origin":    "country name only"\n'
                        "}\n\n"
                        "RULES: Only include values explicitly stated. "
                        "If absent, use null. "
                        "The selling platform (Amazon, Flipkart, etc.) is NOT the marketer. "
                        "Return ONLY valid JSON."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Product: {product_name}\n\nPage text:\n{combined}",
                },
            ],
        )
        raw  = response.choices[0].message.content.strip()
        raw  = re.sub(r"^```(?:json)?\n?", "", raw)
        raw  = re.sub(r"\n?```$", "", raw)
        data = json.loads(raw)

        for key in [
            "manufacturer_name", "manufacturer_address",
            "marketed_by_name", "marketed_by_address", "country_of_origin",
        ]:
            if data.get(key) in [None, "null", "None", "", "N/A", "n/a"]:
                data[key] = ""

        logger.info(
            f"✅ LLM manufacturer: {data.get('manufacturer_name')} | "
            f"country: {data.get('country_of_origin')}"
        )
        return data

    except Exception as exc:
        logger.warning(f"LLM manufacturer extraction failed: {exc}")
        return _regex_manufacturer_fallback(soup, full_text)


def _regex_manufacturer_fallback(soup: BeautifulSoup, full_text: str) -> dict:
    data = {
        "manufacturer_name": "", "manufacturer_address": "",
        "marketed_by_name":  "", "marketed_by_address":  "",
        "country_of_origin": "",
    }
    m = re.search(r"Manufactured\s+[Bb]y\s*[-:]\s*([A-Za-z0-9 .,&\-\(\)]+)", full_text)
    if m:
        data["manufacturer_name"] = m.group(1).strip()[:100]
    m = re.search(r"Marketed\s+[Bb]y\s*[-:]\s*([A-Za-z0-9 .,&\-\(\)]+)", full_text)
    if m:
        data["marketed_by_name"] = m.group(1).strip()[:100]
    m = re.search(r"Country\s+of\s+Origin\s*[-:]\s*([A-Za-z ]+)", full_text, re.IGNORECASE)
    if m:
        data["country_of_origin"] = m.group(1).strip()[:50]
    return data


def build_display(name: str, address: str, country: str = "") -> str:
    """Format manufacturer/marketer name + address into display string."""
    _INVISIBLE = re.compile(r"[\u200e\u200f\u200b\u00ad\ufeff]")

    def _clean(s: str) -> str:
        return _INVISIBLE.sub("", str(s or "")).strip()

    name    = _clean(name)
    address = _clean(address)
    country = _clean(country)

    if not name:
        parts = []
    elif address and address.lower() != name.lower():
        parts = [address] if name.lower() in address.lower() else [name, address]
    else:
        parts = [name]

    combined = " ".join(parts).lower()
    if country and country.lower() not in combined:
        parts.append(country)

    return " | ".join(parts) if parts else "Not available"


# ── Main extraction orchestrator ──────────────────────────────────────

def _extract_all(
    product_soup: BeautifulSoup,
    full_text:    str,
    product_url:  str,
    product_name: str,
    site_name:    str,
) -> dict:
    price, currency = extract_price_universal(product_soup, full_text, product_url)
    mfr_data = extract_manufacturer(product_soup, full_text, product_name)

    manufacturer = (
        build_display(
            mfr_data.get("manufacturer_name", ""),
            mfr_data.get("manufacturer_address", ""),
            mfr_data.get("country_of_origin", ""),
        )
        if mfr_data.get("manufacturer_name") else ""
    )
    marketed_by = build_display(
        mfr_data.get("marketed_by_name", ""),
        mfr_data.get("marketed_by_address", ""),
    )

    description    = extract_description(product_soup, full_text)
    specifications = extract_specifications(product_soup, full_text)

    product_details: dict = {
        "manufacturer":      manufacturer or "Not available",
        "marketed_by":       marketed_by  or "Not available",
        "country_of_origin": mfr_data.get("country_of_origin", "") or "",
        "description":       description,
        "specifications":    specifications,
        "availability":      "in_stock" if price else "unknown",
    }

    if _is_food_or_supplement_page(full_text):
        ingredients      = extract_ingredients_universal(product_soup, full_text)
        supplement_facts = extract_supplement_facts(product_soup, site_name)
        nutrition_facts  = extract_nutrition_facts(product_soup, full_text)

        if ingredients:
            product_details["ingredients"] = ingredients
        if supplement_facts:
            product_details["supplement_facts"] = supplement_facts
        if nutrition_facts:
            product_details["nutrition_facts"] = nutrition_facts

        logger.info(
            f"[{site_name}] 🥗 Food/supplement — "
            f"ingredients={'✅' if ingredients else '❌'}, "
            f"supp={'✅' if supplement_facts else '❌'}, "
            f"nutrition={'✅' if nutrition_facts else '❌'}"
        )

    logger.info(f"[{site_name}] Specs: {len(specifications)} keys | Desc: {len(description)} chars")

    return {
        "price":           price,
        "currency":        currency,
        "url":             product_url,
        "product_details": product_details,
    }


def _empty() -> dict:
    return {
        "price": 0,
        "url":   "",
        "product_details": {
            "manufacturer":   "Not available",
            "marketed_by":    "Not available",
            "description":    "",
            "specifications": {},
            "availability":   "unknown",
        },
    }


# ── Main scrape logic ─────────────────────────────────────────────────

def _scrape_sync(site: dict, product_name: str) -> dict:
    """
    Full scrape pipeline for one site + product:

      Attempt 1: requests  → search page → find product → requests  → product page
      Attempt 2: Playwright → search page → find product → requests  → product page
      Attempt 3: ScraperAPI → search page (only if site is in SCRAPER_API_DOMAINS
                              and SCRAPER_API_KEY is set — handles heavy bot-blockers)
      Attempt 4: Playwright → product page (if product page is blocked on requests)
      Fallback:  Derive product URL from site's URL pattern (Shopify-style sites)
    """
    site_name  = site["name"]
    search_url = build_search_url(
        site["search_url"],
        product_name,
        site.get("encoding", "plus"),
    )
    base_url = site.get("base_url", "")

    logger.info(f"[{site_name}] Product   : {product_name}")
    logger.info(f"[{site_name}] Search URL: {search_url}")

    # ── Dynamic transport selection ──────────────────────────────────
    # Instead of a hardcoded domain list, check if we've previously
    # learned that requests is blocked for this site's domain.
    host = urllib.parse.urlparse(base_url).netloc.lower()
    with _hints_lock:
        transport_hint = _site_transport_hints.get(host, "unknown")

    skip_requests = (transport_hint == "requests_blocked")
    products: list = []

    # ── Attempt 1: requests on search page ───────────────────────────
    if not skip_requests:
        logger.info(f"[{site_name}] Trying requests on search page...")
        search_soup, search_text = _get_soup(search_url, referer=base_url)
        if search_soup:
            page_text = search_soup.get_text().lower()

            # Check for bot block
            if any(k in page_text for k in [
                "enter the characters you see below",
                "robot check",
                "solve this captcha",
                "automated access",
                "are you a human",
                "verify you are human",
                "unusual traffic from your computer",
            ]):
                logger.warning(f"[{site_name}] ⚠️ CAPTCHA/bot-block on requests → learning: skip requests next time")
                with _hints_lock:
                    _site_transport_hints[host] = "requests_blocked"
            else:
                all_products = _extract_product_links(search_soup, search_url)
                if all_products:
                    selected = find_and_validate_product(all_products, product_name)
                    if selected:
                        products = [selected]
                        with _hints_lock:
                            if transport_hint == "unknown":
                                _site_transport_hints[host] = "requests_ok"
                else:
                    # requests got HTML but zero product links →
                    # likely an SPA shell. Skip requests next time.
                    logger.info(f"[{site_name}] requests got 0 products (SPA shell?) → trying Playwright")
                    with _hints_lock:
                        _site_transport_hints[host] = "requests_blocked"
        else:
            logger.info(f"[{site_name}] requests returned empty → trying Playwright")
            with _hints_lock:
                _site_transport_hints[host] = "requests_blocked"
    else:
        logger.info(f"[{site_name}] Skipping requests (previously blocked) → Playwright")

    # ── Attempt 2: Playwright on search page ─────────────────────────
    if not products:
        logger.info(f"[{site_name}] Trying Playwright on search page...")
        pw_soup, pw_text, _ = _get_soup_playwright(search_url)
        if pw_soup:
            # Check for bot detection using exact phrases, not single words.
            # Single words like "blocked" or "confirming" cause false positives
            # on normal product pages.
            pw_lower = pw_text.lower()
            bot_phrases = [
                "please verify you are a human",
                "are you a human",
                "robot check",
                "enter the characters",
                "automated access",
                "access to this page has been denied",
                "unusual traffic from your computer",
                "press and hold to confirm",
                "press & hold to confirm",
            ]
            if any(phrase in pw_lower for phrase in bot_phrases):
                logger.warning(f"[{site_name}] ⚠️ Bot detection triggered on Playwright")
            else:
                all_products = _extract_product_links(pw_soup, search_url)
                selected     = find_and_validate_product(all_products, product_name)
                if selected:
                    products = [selected]

    # ── Attempt 3: ScraperAPI (any site where Playwright is also blocked) ──
    if not products:
        if SCRAPER_API_KEY and SCRAPER_API_KEY.strip():
            logger.info(f"[{site_name}] Playwright blocked — trying ScraperAPI...")
            sa_soup, sa_text = _get_soup_scraperapi(search_url)
            if sa_soup:
                all_products = _extract_product_links(sa_soup, search_url)
                selected     = find_and_validate_product(all_products, product_name)
                if selected:
                    products = [selected]
                    logger.info(f"[{site_name}] ✅ ScraperAPI found product")
            else:
                # Both Playwright and ScraperAPI failed — record it
                with _hints_lock:
                    _site_transport_hints[host] = "playwright_blocked"
        else:
            logger.warning(
                f"[{site_name}] Both requests and Playwright failed. "
                f"Set SCRAPER_API_KEY in .env for fallback — get free key at scraperapi.com"
            )

    # ── Fallback: derive product URL from site's URL pattern ─────────
    if not products:
        logger.warning(f"[{site_name}] ❌ No products from search — trying fallback URL")

        fallback_url = _build_fallback_url(
            base_url, product_name, site.get("search_url", "")
        )

        if not fallback_url:
            logger.warning(f"[{site_name}] ❌ No guessable fallback URL for this site type")
            return {"name": product_name, "listings": []}

        logger.info(f"[{site_name}] Trying fallback URL: {fallback_url}")
        product_soup, full_text = _get_soup(fallback_url)

        if product_soup and len(full_text) > 500:
            result = _extract_all(
                product_soup, full_text, fallback_url, product_name, site_name
            )
            if result.get("price", 0) > 0:
                # Currency detected from the actual page HTML by extract_price_universal
                currency = result.get("currency", "USD")
                symbol   = _symbol_for_currency(currency)
                return {
                    "name": product_name,
                    "listings": [{
                        "platform":        site_name.lower(),
                        "title":           product_name,
                        "price": {
                            "value":    result["price"],
                            "currency": currency,
                            "raw":      f"{symbol}{result['price']}",
                        },
                        "url":             fallback_url,
                        "availability":    result["product_details"].get("availability", "unknown"),
                        "product_details": result["product_details"],
                        "last_updated":    datetime.now(timezone.utc).isoformat(),
                    }],
                }

        return {"name": product_name, "listings": []}

    # ── Found a product — fetch its page ─────────────────────────────
    best_product = products[0]
    product_url  = _clean_product_url(best_product["url"])
    title        = best_product["title"]

    logger.info(f"[{site_name}] Selected: {title[:80]}")
    logger.info(f"[{site_name}] URL: {product_url[:100]}")

    # Try requests first, fall back to Playwright, then ScraperAPI
    product_soup, full_text = _get_soup(product_url, referer=search_url)

    if not product_soup or len(full_text) < 500:
        logger.info(f"[{site_name}] requests blocked on product page → Playwright")
        product_soup, full_text, product_url = _get_soup_playwright(product_url)

    if not product_soup or len(full_text) < 500:
        if SCRAPER_API_KEY and SCRAPER_API_KEY.strip():
            logger.info(f"[{site_name}] Playwright blocked on product page → ScraperAPI")
            product_soup, full_text = _get_soup_scraperapi(product_url)

    if not product_soup or len(full_text) < 500:
        logger.warning(f"[{site_name}] ❌ Could not fetch product page")
        return {"name": product_name, "listings": []}

    logger.info(f"[{site_name}] Product page: {len(full_text)} chars")

    result = _extract_all(product_soup, full_text, product_url, product_name, site_name)

    # If price extraction failed (returns 0), the page might be a bot-blocked
    # shell — Amazon returns pages with specs/description but no buy box.
    # These pass the len>500 check but have no price. Retry with Playwright/ScraperAPI.
    if result.get("price", 0) == 0:
        logger.info(f"[{site_name}] Price=0 on requests page — retrying with Playwright...")
        pw_soup, pw_text, pw_url = _get_soup_playwright(product_url)
        if pw_soup and len(pw_text) > 500:
            result = _extract_all(pw_soup, pw_text, pw_url, product_name, site_name)

    if result.get("price", 0) == 0 and SCRAPER_API_KEY and SCRAPER_API_KEY.strip():
        logger.info(f"[{site_name}] Price=0 on Playwright — retrying with ScraperAPI...")
        sa_soup, sa_text = _get_soup_scraperapi(product_url)
        if sa_soup and len(sa_text) > 500:
            result = _extract_all(sa_soup, sa_text, product_url, product_name, site_name)

    logger.info(f"[{site_name}] Price: {result['price']} {result.get('currency', '?')}")
    logger.info(f"[{site_name}] Mfr  : {str(result['product_details'].get('manufacturer', ''))[:80]}")

    # Currency detected from the actual page HTML by extract_price_universal
    currency = result.get("currency", "USD")
    symbol   = _symbol_for_currency(currency)

    return {
        "name": product_name,
        "listings": [
            {
                "platform":        site_name.lower(),
                "title":           title,
                "price": {
                    "value":    result.get("price", 0),
                    "currency": currency,
                    "raw":      f"{symbol}{result.get('price', 0)}" if result.get("price") else None,
                },
                "url":             result.get("url"),
                "availability":    result["product_details"].get("availability", "unknown"),
                "product_details": result["product_details"],
                "last_updated":    datetime.now(timezone.utc).isoformat(),
            }
        ],
    }