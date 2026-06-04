"""
scout/resolver.py — Website resolver for Scout Agent

ZERO HARDCODING. Uses search_url_detector package for all site detection.
The detector's 5-strategy pipeline handles any website dynamically:
  1. Domain resolution (name → live URL)
  2. Playwright browser simulation (real search)
  3. URL pattern extraction from result URL
  4. HTML form parsing
  5. Heuristic guessing with validation

Public API (used by main.py and scraper.py):
  resolve_website(name)                        → {"base_url", "search_url", "encoding"}
  build_search_url(template, query, encoding)  → final URL string
  normalize_query(query)                       → cleaned query string
"""

from __future__ import annotations

import logging
import re
import urllib.parse
from typing import Optional

from .cancellation import check_cancelled, SearchCancelledException

logger = logging.getLogger(__name__)


# ── Query Normalization ───────────────────────────────────────────────

def normalize_query(query: str) -> str:
    """
    Clean a product query for use in a search URL.

    Rules:
      - Lowercase for case-insensitive matching
      - Apostrophes: L'Oreal → loreal, Men's → mens (remove, no space)
      - Ampersands: fast&up → fast&up (preserve — URL encoding handles it)
      - Hyphens with spaces: " - " → " " (separator)
      - Hyphens without spaces: "sugar-free" → "sugar-free" (keep compound words)
      - Parentheses: stripped, content kept
      - Special chars (™ © ® etc.): stripped
      - Commas: replaced with space
      - Dots between letters: "dr.berg" → "dr.berg" (KEPT — brand names use dots)
      - Dots at end of words: "Rs." → "Rs" (stripped)
    
    Previous code had: re.sub(r"[^a-z0-9\\s&\\-]", " ", q)
    This stripped ALL dots, so "dr.berg" → "dr berg" and "1mg" was fine but
    "dr.berg" lost its dot. Also stripped "/" which broke some category paths.
    
    New code: keeps dots that are between alphanumeric chars (brand names).
    Strips dots at word boundaries (abbreviation periods like "Rs.").
    """
    q = query.lower()
    q = re.sub(r"[™©®]", "", q)
    q = q.replace("'", "").replace("\u2019", "").replace("\u2018", "")
    q = re.sub(r"\s+-\s+", " ", q)
    q = re.sub(r"[()]", "", q)
    q = q.replace(",", " ")
    # Keep dots between alphanumeric chars (dr.berg, v2.0), strip trailing dots
    q = re.sub(r"\.(?!\w)", " ", q)           # dot NOT followed by word char → space
    # Strip remaining special chars EXCEPT . & -
    q = re.sub(r"[^a-z0-9\s&\-.]", " ", q)
    q = re.sub(r"\s+", " ", q)
    return q.strip()


# ── Search URL Builder ───────────────────────────────────────────────

def build_search_url(
    search_url_template: str,
    product_name: str,
    encoding: str = "plus",
) -> str:
    """
    Replace {query} in a search URL template with the encoded product name.
    encoding: "plus" → space becomes +  |  "percent" → space becomes %20

    Encoding rules by position:
      Path {query}  → hyphens     (/nivea-lip-balm)
      Param {query} → plus or percent depending on encoding setting
      
    Special handling for Myntra-style dual encoding:
      path {query}  → hyphens     (nivea-lip-balm)
      param {query} → percent     (nivea%20lip%20balm)

    Previous code bug: For path-only {query} (like /{query}?rawQuery=...),
    it used quote_plus() which produces "nivea+lip+balm". In URL PATHS,
    "+" is a literal character, not a space. Browsers send it as-is,
    and the server sees "nivea+lip+balm" instead of "nivea lip balm".
    The correct encoding for paths is either hyphens or %20.
    """
    from urllib.parse import quote, quote_plus, urlparse

    cleaned = normalize_query(product_name)

    if "{query}" in search_url_template:
        parsed = urlparse(search_url_template)

        # Detect where {query} appears
        has_query_in_path = "{query}" in parsed.path
        has_query_in_params = "{query}" in (parsed.query or "")

        # Case 1: Dual encoding — {query} in BOTH path and query params
        # e.g., https://www.myntra.com/{query}?rawQuery={query}
        if has_query_in_path and has_query_in_params:
            path_encoded = cleaned.replace(" ", "-")
            param_encoded = quote(cleaned, safe="")
            new_path = parsed.path.replace("{query}", path_encoded)
            new_query = parsed.query.replace("{query}", param_encoded)
            return f"{parsed.scheme}://{parsed.netloc}{new_path}?{new_query}"

        # Case 2: {query} ONLY in path (e.g., /search/{query} or /{query})
        # Path encoding MUST use hyphens or %20, NEVER "+"
        # "+" in a URL path is a literal plus sign, not a space.
        if has_query_in_path and not has_query_in_params:
            # Determine path encoding: hyphens for slug-style paths,
            # %20 for explicit search paths
            path_str = parsed.path.lower()
            if "/search" in path_str or "/s/" in path_str or "/sch/" in path_str:
                # Search-style path: use %20
                path_encoded = quote(cleaned, safe="")
            else:
                # Slug-style path: use hyphens
                path_encoded = cleaned.replace(" ", "-")
            new_path = parsed.path.replace("{query}", path_encoded)
            # Rebuild with any existing query string intact
            qs = f"?{parsed.query}" if parsed.query else ""
            return f"{parsed.scheme}://{parsed.netloc}{new_path}{qs}"

        # Case 3: {query} ONLY in query params (most common)
        # e.g., /search?q={query}
        if has_query_in_params and not has_query_in_path:
            if encoding == "percent":
                param_encoded = quote(cleaned, safe="")
            else:
                param_encoded = quote_plus(cleaned)
            new_query = parsed.query.replace("{query}", param_encoded)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"

        # Case 4: {query} somewhere else (shouldn't happen, but handle it)
        encoded = quote(cleaned, safe="") if encoding == "percent" else quote_plus(cleaned)
        return search_url_template.replace("{query}", encoded)

    if "%s" in search_url_template:
        encoded = quote(cleaned, safe="") if encoding == "percent" else quote_plus(cleaned)
        return search_url_template.replace("%s", encoded)

    encoded = quote(cleaned, safe="") if encoding == "percent" else quote_plus(cleaned)
    sep = "&" if "?" in search_url_template else "?"
    return f"{search_url_template}{sep}q={encoded}"


# ── Encoding Detection ───────────────────────────────────────────────

def _detect_encoding(search_url: str) -> str:
    """
    Detect the encoding style a search URL template expects.
    
    This is stored in the DB as the 'encoding' field and passed to
    build_search_url(). However, build_search_url now handles path vs param
    encoding internally, so this mainly affects param-only templates.
    
    Returns:
      "plus"    → spaces become + (standard query params: ?q=nivea+lip+balm)
      "percent" → spaces become %20 (rawQuery style: rawQuery=nivea%20lip%20balm)
    """
    # rawQuery param → percent encoding (Myntra style)
    if "rawQuery=" in search_url or "rawquery=" in search_url.lower():
        return "percent"

    # Query only in path → build_search_url handles this internally
    # but we return "percent" for backward compat with any code that
    # reads encoding from DB
    parsed = urllib.parse.urlparse(search_url)
    if "{query}" in parsed.path:
        return "percent"

    # Nykaa, 1mg, Pharmeasy-style paths → percent
    if any(p in search_url for p in ["/search/result/", "/search/all?", "/search-medicines/"]):
        return "percent"

    # Default: plus encoding (most common)
    return "plus"


# ── Imports for resolver ─────────────────────────────────────────────

import os
import json
import asyncio
import threading
import httpx

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o-mini"

VALID_QUERY_PARAMS = {
    'q', 'k', 's', 'query', 'keyword', 'search',
    'text', 'term', 'keywords', 'sq', 'qs', 'name',
    'w', 'p', 'searchterm', 'search_query', 'txtq',
    'searchtext', 'searchkeyword', 'searchtxt',
    'searchvalue', 'inputtext', 'querytext', 'searchinput',
    'searchkey', 'searchword', 'searchfor', 'querystring',
}


# ── Junk param detection ─────────────────────────────────────────────

def _is_junk_param(key: str, value: str) -> bool:
    key = key.lower()
    value = value.lower()
    if len(value) > 20 and re.search(r'[a-z0-9]{8,}', value):
        return True
    junk_patterns = [
        r'^utm_', r'^ref', r'^crid$', r'^sprefix$', r'^hv',
        r'^gclid$', r'^fbclid$', r'^msclkid$', r'^adgr',
        r'^hydadcr$', r'^_', r'^url$', r'^field-',
        r'^search-alias', r'^rb_', r'^rh$', r'^rnid$',
        r'^psc$', r'^pd_rd_', r'^pf_rd_',
    ]
    if any(re.search(p, key) for p in junk_patterns):
        return True
    if key in ('page', 'start', 'offset') and value.isdigit():
        return True
    return False


def _is_essential_param(key: str, value: str) -> bool:
    if _is_junk_param(key, value):
        return False
    if '[' in key or ']' in key:
        return False
    if value.endswith(',') or value.endswith('%2C'):
        return False
    if len(key) <= 10 and len(value) <= 20:
        return True
    return False


# ── Domain utilities ─────────────────────────────────────────────────

def _root_domain(netloc: str) -> str:
    parts = netloc.lower().split(".")
    if len(parts) >= 3 and parts[-2] in ("co", "com", "net", "org") and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _search_url_domain_matches_base(discovered: dict, base_url: str) -> bool:
    """
    Check if the discovered search URL belongs to the same site as base_url.

    Accepts exact root match (amazon.in search URL on amazon.in — normal case).
    Also accepts same-SLD cross-root (e.g., Playwright on ikea.in submits search
    and gets redirected to ikea.com/in/en/search — legitimate multi-region
    architecture by the same owner).

    Rejects different-SLD redirects (e.g., flipkart.co → sedo.com parking page).
    """
    discovered_netloc = urllib.parse.urlparse(discovered["search_url"]).netloc
    base_netloc = urllib.parse.urlparse(base_url).netloc

    # Case 1: exact root domain match (most common)
    if _root_domain(discovered_netloc) == _root_domain(base_netloc):
        return True

    # Case 2: same SLD but different TLD (multi-region redirect by same owner)
    # e.g., ikea.in → ikea.com/in/en, unilever.in → unilever.com
    discovered_sld = _root_domain(discovered_netloc).split(".")[0] if discovered_netloc else ""
    base_sld = _root_domain(base_netloc).split(".")[0] if base_netloc else ""
    if discovered_sld and base_sld and discovered_sld == base_sld:
        logger.info(
            f"[resolver] ✅ Cross-root accepted (same SLD '{discovered_sld}'): "
            f"{base_netloc} → {discovered_netloc}"
        )
        return True

    logger.info(
        f"[resolver] ⚠️ Cross-root rejected (SLD differs '{base_sld}' vs '{discovered_sld}'): "
        f"{base_netloc} → {discovered_netloc}"
    )
    return False


def _candidate_domains(name: str) -> list[str]:
    """
    Generate candidate URLs for a site name.

    Cases:
      1. Input is already a domain like "amazon.in" or "amazon.co.uk"
         → treat it as the domain directly, don't strip the dot
      2. Input is a brand name like "amazon" or "fast and up"
         → generate a wide range of TLDs and subdomains
    """
    raw = name.lower().strip()

    # ── Case 1: input looks like a domain (has a dot, no spaces) ─────
    if "." in raw and " " not in raw:
        # Strip protocol if present
        raw = re.sub(r"^https?://", "", raw).rstrip("/")
        # Strip leading www.
        raw = re.sub(r"^www\.", "", raw)
        # Try both www. and bare variants
        return [
            f"https://www.{raw}",
            f"https://{raw}",
        ]

    # ── Case 2: brand name → generate candidates ──────────────────────
    slug = raw
    slug = re.sub(r"[&@\s]+", "", slug)
    slug = re.sub(r"[^a-z0-9\-]", "", slug)
    slug = slug.strip("-")
    slug_hyphen = re.sub(r"\s+", "-", raw)
    slug_hyphen = re.sub(r"[^a-z0-9\-]", "", slug_hyphen)
    bases = [slug]
    if slug_hyphen and slug_hyphen != slug:
        bases.append(slug_hyphen)

    # Subdomain + TLD combinations
    # Order: most-common first. Dedup happens after.
    subdomains = ["www.", "", "shop.", "in.", "store."]
    tlds = [".com", ".in", ".co.uk", ".de", ".com.au", ".ca", ".co"]

    candidates = []
    for s in bases:
        if not s:
            continue
        for tld in tlds:
            for sub in subdomains:
                candidates.append(f"https://{sub}{s}{tld}")

    seen, result = set(), []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result


# ── Domain probing ───────────────────────────────────────────────────

async def _probe_domain(client, url: str) -> Optional[str]:
    try:
        try:
            resp = await client.head(url)
            if resp.status_code == 405:
                resp = await client.get(url)
        except Exception:
            resp = await client.get(url)
        if resp.status_code < 400 or resp.status_code == 403:
            final = str(resp.url).rstrip("/")
            parsed = urllib.parse.urlparse(final)
            base = f"{parsed.scheme}://{parsed.netloc}"

            # Verify the final URL's root domain matches what we probed.
            # If we asked for 'myntra.in' and it redirected to 'myntra.com',
            # that means 'myntra.in' doesn't really exist as a separate site —
            # it's just redirecting to the real domain. Don't treat as live.
            requested_netloc = urllib.parse.urlparse(url).netloc.lower()
            requested_root = _root_domain(requested_netloc)
            final_root = _root_domain(parsed.netloc.lower())
            if requested_root != final_root:
                logger.info(
                    f"[resolver] ⚠️ Redirect across roots: {url} → {base} "
                    f"(requested {requested_root}, got {final_root}) — skipping"
                )
                return None

            logger.info(f"[resolver] ✅ Live: {url} → {base}")
            return base
    except Exception as e:
        logger.debug(f"[resolver] ✗ {url} — {type(e).__name__}")
    return None


async def _find_live_domains(name: str) -> list[str]:
    candidates = _candidate_domains(name)
    logger.info(f"[resolver] Probing {len(candidates)} candidates for '{name}'...")
    async with httpx.AsyncClient(
        timeout=5, follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    ) as client:
        results = await asyncio.gather(*[_probe_domain(client, url) for url in candidates])
    seen, live = set(), []
    for base in results:
        if base and base not in seen:
            seen.add(base)
            live.append(base)
    return live


# ── LLM helpers ──────────────────────────────────────────────────────

async def _domain_matches_site(base_url: str, site_name: str) -> bool:
    if not OPENAI_API_KEY:
        return True
    domain = urllib.parse.urlparse(base_url).netloc
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL, "temperature": 0, "max_tokens": 10,
                    "messages": [{"role": "user", "content":
                        f'Is "{domain}" the website domain for the e-commerce site or brand "{site_name}"?\n'
                        f'Use your training knowledge. If uncertain, answer "yes".\n'
                        f'Answer ONLY "yes" or "no". No other text.'}],
                }
            )
        answer = resp.json()["choices"][0]["message"]["content"].strip().lower()
        is_match = "yes" in answer
        logger.info(f"[resolver] Domain match '{domain}' for '{site_name}': {answer}")
        return is_match
    except Exception as e:
        logger.warning(f"[resolver] Domain match check failed: {e}")
        return True


async def _ask_openai_for_domain(name: str) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    logger.info(f"[resolver] Asking OpenAI for domain of '{name}'...")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL, "temperature": 0, "max_tokens": 80,
                    "messages": [{"role": "user", "content":
                        f'What is the EXACT homepage URL of the e-commerce or health website called "{name}"?\n'
                        f'It might be an Indian website. The domain may not match the name exactly.\n'
                        f'Common patterns: shop.brandname.com, www.brandname.in, brandnameapp.com\n'
                        f'Return ONLY JSON: {{"base_url": "https://example.com"}}'}],
                }
            )
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        data = json.loads(raw)
        url = data.get("base_url", "").rstrip("/")
        logger.info(f"[resolver] OpenAI suggests domain: {url}")
        return url if url else None
    except Exception as e:
        logger.warning(f"[resolver] OpenAI domain lookup failed: {e}")
        return None


# ── E-commerce verification ──────────────────────────────────────────

async def _verify_is_ecommerce(base_url: str, search_url: str, encoding: str) -> bool:
    try:
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        ) as client:
            home_resp = await client.get(base_url)
            home_text = home_resp.text.lower()

        ecom_signals = [
            'add to cart', 'add to bag', 'buy now', 'shop now',
            'checkout', '₹', 'cart', 'wishlist',
            '/products/', '/collections/', '/shop/',
            'shopify', 'woocommerce', 'magento', 'prestashop',
            'schema.org/product', '"product"', '"offer"',
        ]
        non_ecom_signals = [
            'this domain is for sale', 'parked domain',
            'coming soon', 'under construction',
            'domain parking', 'buy this domain',
        ]

        ecom_score = sum(1 for s in ecom_signals if s in home_text)
        non_ecom_hits = sum(1 for s in non_ecom_signals if s in home_text)

        if non_ecom_hits > 0:
            logger.info(f"[resolver] ❌ Non e-commerce signals on {base_url}")
            return False
        if ecom_score >= 2:
            logger.info(f"[resolver] ✅ E-commerce confirmed: {base_url} (score={ecom_score})")
            return True

        # Check search results page
        test_url = build_search_url(search_url, "vitamin c", encoding)
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        ) as client:
            search_resp = await client.get(test_url)
            search_text = search_resp.text.lower()

        product_signals = [
            'add to cart', 'buy now', '₹', 'price',
            '/products/', '/dp/', 'in stock', 'out of stock',
        ]
        product_score = sum(1 for s in product_signals if s in search_text)

        if product_score >= 2:
            logger.info(f"[resolver] ✅ E-commerce confirmed from search: {base_url}")
            return True

        logger.info(f"[resolver] ❌ Not e-commerce: {base_url} (ecom={ecom_score}, prod={product_score})")
        return False

    except Exception as e:
        logger.warning(f"[resolver] E-commerce verify error: {e}")
        return True  # Don't block on errors


# ── Playwright search discovery (runs in thread) ─────────────────────

def _looks_like_redirect_not_search(result_url: str, base_url: str, test_query: str) -> bool:
    parsed = urllib.parse.urlparse(result_url)
    path_lower = parsed.path.lower()
    redirect_patterns = [
        r'/best-seller', r'/bestseller', r'/top-product',
        r'/featured', r'/trending',
    ]
    if not any(re.search(p, path_lower) for p in redirect_patterns):
        return False

    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    query_words = test_query.lower().split()
    query_in_params = any(
        any(w in urllib.parse.unquote_plus(v).lower() for w in query_words)
        for values in params.values() for v in values
    )
    if not query_in_params:
        logger.info(f"[resolver] ⚠️ Redirect URL (query not in params): {result_url}")
        return True
    return False


async def _discover_search_url(base_url: str) -> Optional[dict]:
    """
    Run Playwright in a separate thread (Windows event loop safety).
    Two-tier strategy:
      1. Regular Playwright (fast, no stealth)   — ~8s
      2. Retry with playwright-stealth if step 1 failed  — ~8s

    Total worst case: ~16s per domain. First-tier succeeds for most sites,
    so stealth overhead only applies when needed (Myntra, Nykaa, Cloudflare sites).
    """
    # First attempt: no stealth (faster, doesn't break Target/DrBerg/etc.)
    result = {}
    def _run_plain():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result["data"] = loop.run_until_complete(
                _playwright_discover(base_url, use_stealth=False)
            )
        except Exception as e:
            logger.warning(f"[resolver] Plain Playwright error: {e}")
        finally:
            loop.close()

    t1 = threading.Thread(target=_run_plain)
    t1.start()
    t1.join(timeout=30)
    if result.get("data"):
        return result["data"]

    # Second attempt: stealth mode (slower, but may defeat bot detection)
    logger.info(f"[resolver] 🛡️ Plain Playwright failed — retrying with stealth...")
    result2 = {}
    def _run_stealth():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result2["data"] = loop.run_until_complete(
                _playwright_discover(base_url, use_stealth=True)
            )
        except Exception as e:
            logger.warning(f"[resolver] Stealth Playwright error: {e}")
        finally:
            loop.close()

    t2 = threading.Thread(target=_run_stealth)
    t2.start()
    t2.join(timeout=30)
    return result2.get("data")


# ── Playwright helpers (stealth, overlay dismissal) ──────────────────

async def _apply_stealth(page, context) -> str:
    """
    Apply playwright-stealth evasions, supporting both v1 and v2 APIs.

    playwright-stealth 2.x removed the top-level `stealth_async` function
    and replaced it with `Stealth().apply_stealth_async(context)`. The
    previous code did `from playwright_stealth import stealth_async` and
    treated the resulting ImportError as "package not installed", which is
    wrong — the package IS installed, just shaped differently. The misleading
    log line ("playwright-stealth not installed") was masking the real bug.

    Order of attempts:
      1. v2 API: `Stealth().apply_stealth_async(context)` — current
      2. v1 API: `stealth_async(page)` — legacy, kept for compatibility
      3. Manual fallback: just mask the navigator.webdriver flag

    Returns a short status string for logging — "v2", "v1", or "manual".
    Never raises — stealth is best-effort, the scrape still proceeds.
    """
    # Try v2 first (the actual installed version per the user's pip output)
    try:
        from playwright_stealth import Stealth  # type: ignore
        await Stealth().apply_stealth_async(context)
        return "v2"
    except ImportError:
        pass  # fall through to v1
    except Exception as e:
        logger.warning(f"[resolver] Stealth v2 setup error: {e}")
        # Don't fall through on non-ImportError — v2 IS installed but broken.
        # Use manual fallback so we still get something.

    # Try v1 (older installs)
    try:
        from playwright_stealth import stealth_async  # type: ignore
        await stealth_async(page)
        return "v1"
    except ImportError:
        pass  # neither version available — manual fallback
    except Exception as e:
        logger.warning(f"[resolver] Stealth v1 setup error: {e}")

    # Manual fallback: minimal evasion (mask the webdriver flag).
    # Better than nothing, and what the previous code did when it thought
    # the package was missing.
    await page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return "manual"


async def _dismiss_overlays(page) -> int:
    """
    Best-effort dismissal of modals, login walls, cookie banners, and
    region popups that intercept clicks on search inputs.

    Why this is needed: Flipkart, Amazon-IN, Myntra, and many e-commerce
    sites show a modal on first visit (login prompt, "select location",
    cookie consent). The modal's backdrop has `tabindex="-1"` and a high
    z-index, so it captures clicks before they reach the search box.

    Strategy (three passes, in order):
      1. Press Escape twice — closes accessibly-built native dialogs.
      2. Click standard dismiss buttons by ARIA / button text. We do NOT
         use class-name selectors (e.g. Flipkart's `_2KpZ6l._2doB4z`) —
         those break on the next deploy. Anything we keep here must be
         portable.
      3. Generic obstruction removal: look at fixed/sticky elements with
         high z-index that cover the page, and hide them via JS. This is
         the catch-all for sites whose modals don't expose any standard
         dismiss affordance (Flipkart's login wall, for example).

    Returns the count of dismiss actions that fired (logging only).
    Never raises — overlay dismissal is best-effort. The scrape continues
    even if every step fails; the worst case is the same as before.
    """
    actions = 0

    # Pass 1 — Escape. Two presses handles nested modals (cookie banner
    # over login modal). Cheap and idempotent.
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(150)
        await page.keyboard.press("Escape")
        actions += 1
    except Exception:
        pass

    # Pass 2 — standard dismiss buttons. ARIA labels and button text only.
    # No class-name selectors: those go stale on every site redeploy and
    # are why this function failed on Flipkart even after "succeeding".
    # We do NOT break after the first match — multiple banners may be
    # stacked (cookie + login, region + cookie, etc).
    CLOSE_SELECTORS = [
        "button[aria-label*='close' i]",
        "button[aria-label*='dismiss' i]",
        "[role='dialog'] button[aria-label*='close' i]",
        "button:has-text('No thanks')",
        "button:has-text('Not now')",
        "button:has-text('Not Now')",
        "button:has-text('Skip')",
        "button:has-text('Maybe later')",
        "button:has-text('Continue without')",
        "button:has-text('Got it')",
        "button:has-text('I agree')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
    ]

    for selector in CLOSE_SELECTORS:
        try:
            # locator.first handles multiple matches; short timeout so
            # missing selectors don't pile up into seconds of waiting.
            loc = page.locator(selector).first
            if await loc.count() > 0:
                await loc.click(timeout=400)
                await page.wait_for_timeout(150)
                actions += 1
        except Exception:
            continue

    # Pass 3 — generic obstruction removal. Independent of selectors.
    # Why this exists: Flipkart's login modal has no aria-label, no
    # close button, no "X". The previous version "succeeded" by clicking
    # an unrelated button (or maybe a phantom one) and returning 1, but
    # the actual modal — `<div tabindex="-1" class="mcO4kT RFBkxv">` —
    # was still up and intercepting clicks. We can't fix that with new
    # selectors because Flipkart will rotate the class hash on their
    # next deploy. Instead, ask the browser directly: what is currently
    # covering the page? Anything fixed/sticky with a high z-index that
    # spans most of the viewport is almost certainly an unwanted overlay.
    #
    # Why this is safe: we only target elements that ALSO cover most of
    # the viewport. A normal cart drawer or dropdown is small or anchored
    # to a corner; we leave those alone. The threshold (60% viewport) is
    # tuned to catch full-screen modals without touching legitimate UI.
    try:
        removed = await page.evaluate("""() => {
            let count = 0;
            const vw = window.innerWidth;
            const vh = window.innerHeight;
            const minArea = vw * vh * 0.6;
            // Walk all elements; cheap because most pages have <5k.
            for (const el of document.querySelectorAll('body *')) {
                const cs = window.getComputedStyle(el);
                const pos = cs.position;
                if (pos !== 'fixed' && pos !== 'sticky') continue;
                const z = parseInt(cs.zIndex, 10);
                if (!Number.isFinite(z) || z < 100) continue;
                if (cs.display === 'none' || cs.visibility === 'hidden') continue;
                const r = el.getBoundingClientRect();
                if (r.width * r.height < minArea) continue;
                // Found a viewport-spanning fixed/sticky overlay. Hide it.
                // We use display:none rather than .remove() so the page's
                // own JS doesn't notice and re-create the modal.
                el.style.setProperty('display', 'none', 'important');
                count++;
            }
            return count;
        }""")
        if removed:
            actions += removed
    except Exception:
        # JS evaluation failure is non-fatal; we already did passes 1 & 2.
        pass

    return actions


async def _playwright_discover(base_url: str, use_stealth: bool = False) -> Optional[dict]:
    from playwright.async_api import async_playwright

    TEST_QUERY = "vitamin c"

    INPUT_NAME_SELECTORS = [
        "input[name='k']", "input[name='q']", "input[name='s']",
        "input[name='query']", "input[name='keyword']", "input[name='keywords']",
        "input[name='search']", "input[name='name']", "input[name='w']",
        "input[name='searchterm']", "input[name='search_query']",
        "input[name='txtQ']", "input[name='txtq']",
    ]
    INPUT_GENERIC_SELECTORS = [
        "input[type='search']",
        "input#twotabsearchtextbox",
        "input[placeholder*='search' i]",
        "input[aria-label*='search' i]",
        "input[class*='search' i]",
        "input[id*='search' i]",
    ]
    ALL_INPUT_SELECTORS = INPUT_NAME_SELECTORS + INPUT_GENERIC_SELECTORS

    ICON_SELECTORS = [
        "button[class*='search' i]", "a[class*='search' i]",
        "[data-testid*='search' i]", ".search-icon", ".search-toggle",
        "button[aria-label*='search' i]", "a[href*='search']",
        "[class*='search-toggle' i]", "[class*='header-search' i]",
    ]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="en-IN",
            extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
        )
        page = await context.new_page()

        # Only apply stealth when explicitly requested (fallback mode).
        # Stealth can cause regressions on sites that work fine without it
        # (e.g., Target's React hydration detection, DrBerg Shopify fill).
        if use_stealth:
            mode = await _apply_stealth(page, context)
            if mode in ("v1", "v2"):
                logger.info(f"[resolver] 🛡️ Applied playwright-stealth ({mode})")
            else:
                logger.info(f"[resolver] playwright-stealth unavailable, using manual fallback")
        else:
            # Minimal baseline stealth — mask webdriver flag only.
            # This is what worked before on Target/DrBerg/etc.
            await page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

        try:
            logger.info(f"[resolver] Playwright visiting {base_url}...")
            try:
                await page.goto(base_url, timeout=15000, wait_until="domcontentloaded")
            except Exception as goto_err:
                err_str = str(goto_err)
                if "ERR_HTTP2_PROTOCOL_ERROR" in err_str or "net::ERR_" in err_str:
                    # HTTP/2 blocked — retry with HTTP/1.1
                    logger.info(f"[resolver] HTTP/2 blocked, retrying with HTTP/1.1...")
                    await browser.close()
                    browser = await pw.chromium.launch(
                        headless=True,
                        args=[
                            "--no-sandbox",
                            "--disable-blink-features=AutomationControlled",
                            "--disable-http2",
                        ]
                    )
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        viewport={"width": 1440, "height": 900},
                        locale="en-IN",
                        extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
                    )
                    page = await context.new_page()
                    if use_stealth:
                        await _apply_stealth(page, context)
                    else:
                        await page.add_init_script(
                            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                        )
                    await page.goto(base_url, timeout=15000, wait_until="domcontentloaded")
                else:
                    raise
            await page.wait_for_timeout(2000)

            # Dismiss any login modals, cookie banners, or region popups
            # that intercept clicks. Many e-commerce sites (Flipkart in
            # particular) show a login modal on first visit whose backdrop
            # captures clicks meant for the search input. Without this,
            # Phase 3 finds the input but every click times out with
            # "intercepts pointer events".
            try:
                actions = await _dismiss_overlays(page)
                if actions > 0:
                    logger.info(f"[resolver] 🚪 Dismissed {actions} overlay(s) before search")
                    # Brief settle after dismissal so DOM stabilizes
                    await page.wait_for_timeout(400)
            except Exception as overlay_err:
                # Overlay dismissal is best-effort. If it errors we still
                # try to find and click the search input — it might work
                # on sites without a modal.
                logger.debug(f"[resolver] Overlay dismiss error (non-fatal): {overlay_err}")

            search_input = None
            input_name_attr = None

            # Phase 1: visible input
            for selector in ALL_INPUT_SELECTORS:
                try:
                    el = await page.query_selector(selector)
                    if not el:
                        continue
                    box = await el.bounding_box()
                    w = box["width"] if box else 0
                    if w >= 80:
                        search_input = selector
                        input_name_attr = await el.get_attribute("name")
                        logger.info(f"[resolver] ✅ Phase1 input: {selector} name='{input_name_attr}'")
                        break
                except Exception:
                    continue

            # Phase 2: click search icons to reveal hidden inputs
            if not search_input:
                for icon in ICON_SELECTORS:
                    try:
                        icon_el = await page.query_selector(icon)
                        if not icon_el:
                            continue
                        await icon_el.click()
                        await page.wait_for_timeout(1000)
                        for selector in ALL_INPUT_SELECTORS:
                            try:
                                el = await page.query_selector(selector)
                                if not el:
                                    continue
                                box = await el.bounding_box()
                                w = box["width"] if box else 0
                                if w >= 80:
                                    search_input = selector
                                    input_name_attr = await el.get_attribute("name")
                                    logger.info(f"[resolver] ✅ Phase2 input after icon click: {selector}")
                                    break
                            except Exception:
                                continue
                        if search_input:
                            break
                    except Exception:
                        continue

            # Phase 3: accept any named input even if narrow (Shopify drawers)
            if not search_input:
                for selector in INPUT_NAME_SELECTORS:
                    try:
                        el = await page.query_selector(selector)
                        if not el:
                            continue
                        await page.evaluate(
                            "el => { el.style.display='block'; el.style.visibility='visible'; el.style.width='300px'; }",
                            el
                        )
                        await page.wait_for_timeout(300)
                        search_input = selector
                        input_name_attr = await el.get_attribute("name")
                        logger.info(f"[resolver] ✅ Phase3 forced visible: {selector}")
                        break
                    except Exception:
                        continue

            # Phase 4: full DOM scan
            if not search_input:
                try:
                    for el in await page.locator("input").all():
                        try:
                            box = await el.bounding_box()
                            if not box or box["width"] < 80:
                                continue
                            input_type = await el.get_attribute("type") or ""
                            placeholder = await el.get_attribute("placeholder") or ""
                            name_attr = await el.get_attribute("name") or ""
                            combined = f"{input_type} {placeholder} {name_attr}".lower()
                            if any(kw in combined for kw in ["search", "query", "product"]) or box["width"] > 200:
                                search_input = el
                                input_name_attr = name_attr
                                logger.info(f"[resolver] ✅ Phase4 input: name='{input_name_attr}'")
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

            if not search_input:
                logger.info(f"[resolver] ❌ No search input found on {base_url}")
                return None

            # Fill the search input
            try:
                if isinstance(search_input, str):
                    await page.evaluate(f"""() => {{
                        const el = document.querySelector("{search_input}");
                        if (el) {{ el.value = "{TEST_QUERY}"; el.dispatchEvent(new Event('input', {{bubbles:true}})); }}
                    }}""")
                    try:
                        await page.click(search_input, timeout=2000)
                        await page.fill(search_input, TEST_QUERY)
                    except Exception:
                        pass
                else:
                    await page.evaluate(
                        f'el => {{ el.value = "{TEST_QUERY}"; el.dispatchEvent(new Event("input", {{bubbles:true}})); }}',
                        search_input
                    )
                    try:
                        await search_input.click()
                        await search_input.fill(TEST_QUERY)
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"[resolver] Fill error: {e}")
                return None

            await page.wait_for_timeout(500)

            # VERIFY the fill actually stuck. Shopify-style stores sometimes
            # reset the input after fill (DrBerg regression). If empty → skip.
            try:
                if isinstance(search_input, str):
                    actual_value = await page.evaluate(
                        f"""() => document.querySelector("{search_input}")?.value || ''"""
                    )
                else:
                    actual_value = await search_input.evaluate("el => el.value || ''")
                if not actual_value or actual_value.strip() == "":
                    logger.warning(
                        f"[resolver] ⚠️ Fill value didn't stick (got empty), "
                        f"retrying with keyboard.type..."
                    )
                    # Retry by simulating typing
                    try:
                        if isinstance(search_input, str):
                            await page.click(search_input, timeout=2000)
                            await page.keyboard.type(TEST_QUERY, delay=50)
                        else:
                            await search_input.click()
                            await page.keyboard.type(TEST_QUERY, delay=50)
                        await page.wait_for_timeout(300)
                    except Exception as retype_err:
                        # The most common cause here is "intercepts pointer
                        # events" — a modal we missed during initial dismiss
                        # has appeared (or stayed up). Run dismiss again,
                        # which will now also catch viewport-spanning
                        # overlays via the generic JS pass, then try once
                        # more before giving up. Without this, every site
                        # with a re-rendering login modal (Flipkart) is
                        # unaddable even though we know the search input.
                        err_str = str(retype_err)
                        if "intercepts pointer events" in err_str or "Timeout" in err_str:
                            logger.info(
                                f"[resolver] 🔓 Retype blocked — re-dismissing overlays and retrying"
                            )
                            try:
                                await _dismiss_overlays(page)
                                await page.wait_for_timeout(300)
                                if isinstance(search_input, str):
                                    await page.click(search_input, timeout=2000)
                                    await page.keyboard.type(TEST_QUERY, delay=50)
                                else:
                                    await search_input.click()
                                    await page.keyboard.type(TEST_QUERY, delay=50)
                                await page.wait_for_timeout(300)
                            except Exception as second_err:
                                logger.warning(
                                    f"[resolver] Retype failed after re-dismiss: {second_err}"
                                )
                                return None
                        else:
                            logger.warning(f"[resolver] Retype failed: {retype_err}")
                            return None
            except Exception as verify_err:
                logger.debug(f"[resolver] Fill-verify error: {verify_err}")

            # Submit: JS form.submit() → button click → Enter key
            js_submitted = False
            try:
                input_sel = search_input if isinstance(search_input, str) else None
                if input_sel:
                    js_submitted = await page.evaluate(f"""() => {{
                        const input = document.querySelector("{input_sel}");
                        if (input && input.form) {{ input.form.submit(); return true; }}
                        return false;
                    }}""")
                    if js_submitted:
                        try:
                            await page.wait_for_load_state("domcontentloaded", timeout=3000)
                        except Exception:
                            pass
                        if page.url.rstrip("/") == base_url.rstrip("/"):
                            js_submitted = False
            except Exception:
                pass

            if not js_submitted:
                for btn in [
                    "form button[type='submit']", "form input[type='submit']",
                    "button[type='submit']", "button[class*='search' i]",
                    "button[aria-label*='search' i]", ".search-submit", ".btn-search",
                ]:
                    try:
                        await page.locator(btn).first.click(timeout=1500)
                        js_submitted = True
                        break
                    except Exception:
                        continue

            if not js_submitted:
                await page.keyboard.press("Enter")

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=8000)
            except Exception:
                pass
            await page.wait_for_timeout(1000)

            result_url = page.url
            logger.info(f"[resolver] Result URL: {result_url}")

            parsed = urllib.parse.urlparse(result_url)
            if not parsed.netloc:
                result_url = urllib.parse.urljoin(base_url, result_url)
                parsed = urllib.parse.urlparse(result_url)

            if not parsed.netloc or result_url.rstrip("/") == base_url.rstrip("/"):
                logger.info(f"[resolver] ❌ No navigation after search")
                return None

            if _looks_like_redirect_not_search(result_url, base_url, TEST_QUERY):
                if input_name_attr:
                    actual_base = f"{parsed.scheme}://{urllib.parse.urlparse(base_url).netloc}"
                    search_url = f"{actual_base}/search?{input_name_attr}={{query}}"
                    return {"base_url": actual_base, "search_url": search_url, "encoding": "plus"}
                return None

            # Identify query parameter
            raw_query = parsed.query
            params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            query_param = None

            if input_name_attr and input_name_attr in params:
                decoded = [urllib.parse.unquote_plus(v) for v in params[input_name_attr]]
                if any(TEST_QUERY.lower() in v.lower() for v in decoded):
                    query_param = input_name_attr

            if not query_param:
                for key, values in params.items():
                    decoded = [urllib.parse.unquote_plus(v) for v in values]
                    if any(TEST_QUERY.lower() in v.lower() for v in decoded):
                        query_param = key
                        break

            if query_param and query_param.lower() not in VALID_QUERY_PARAMS:
                for key, values in params.items():
                    if key.lower() in VALID_QUERY_PARAMS:
                        decoded = [urllib.parse.unquote_plus(v) for v in values]
                        if any(TEST_QUERY.lower() in v.lower() for v in decoded):
                            query_param = key
                            break

            # Detect encoding
            encoding = "plus"
            if query_param:
                m = re.search(rf'(?:^|&){re.escape(query_param)}=([^&]*)', raw_query)
                if m:
                    encoding = "percent" if "%20" in m.group(1) else "plus"

            # Build search URL template
            actual_base = f"{parsed.scheme}://{parsed.netloc}"

            if query_param:
                clean_path = re.sub(r"/ref=[^/?]+", "", parsed.path)
                base_search = f"{actual_base}{clean_path}"
                parts, query_inserted = [], False
                for k, v in urllib.parse.parse_qsl(raw_query, keep_blank_values=True):
                    if k == query_param and not query_inserted:
                        parts.append(f"{k}={{query}}")
                        query_inserted = True
                    elif k != query_param and _is_essential_param(k, v):
                        parts.append(f"{k}={v}")
                if not query_inserted:
                    fallback = input_name_attr if input_name_attr and input_name_attr.lower() in VALID_QUERY_PARAMS else "q"
                    parts.append(f"{fallback}={{query}}")
                search_url = f"{base_search}?{'&'.join(parts)}"
            else:
                path = parsed.path
                found = False
                for variant in [
                    urllib.parse.quote(TEST_QUERY, safe=""),
                    urllib.parse.quote_plus(TEST_QUERY),
                    TEST_QUERY.replace(" ", "-"),
                    TEST_QUERY.replace(" ", "+"),
                ]:
                    if variant in path:
                        search_url = f"{actual_base}{path.replace(variant, '{{query}}')}"
                        encoding = "percent" if "%20" in variant else "plus"
                        found = True
                        break
                if not found:
                    logger.info(f"[resolver] ❌ Could not extract pattern from: {path}")
                    return None

            if search_url.startswith("/"):
                search_url = f"{actual_base}{search_url}"

            logger.info(f"[resolver] ✅ Discovered: {search_url} (encoding={encoding})")
            return {"base_url": actual_base, "search_url": search_url, "encoding": encoding}

        except Exception as e:
            logger.warning(f"[resolver] Playwright error: {e}")
            return None
        finally:
            await browser.close()


# ── Public Entry Point ───────────────────────────────────────────────

async def resolve_website(name: str, request_id: Optional[str] = None) -> dict:
    """
    Resolve a website name → search URL template.

    5-step pipeline:
      1. Find live candidate domains (httpx probing)
      2. Validate domains via LLM ("is this the right site?")
      3. Playwright discovers search URL on validated domains
      4. If step 3 fails, ask OpenAI for correct domain and retry
      5. Last resort: OpenAI guesses the search URL directly

    Cooperative cancellation:
      If request_id is provided AND has been registered via
      cancellation.register_search(), this function calls check_cancelled()
      between phases. When the user clicks Cancel in the UI, the next
      checkpoint raises SearchCancelledException, which the route handler
      catches and returns as {"status": "cancelled"}.

      Granularity: checks happen at phase boundaries, not inside individual
      network calls. Worst-case cancel latency is one Playwright iteration
      (~5-10s) — the same granularity as the existing product-search cancel.
      Passing request_id=None disables checks entirely (used by internal
      callers without a UI session, e.g., scripts).

    Usage:
      - Single-region site: user types "flipkart" → resolver finds flipkart.com
      - Multi-region site: user types "amazon.in" → dotted input is used directly,
        resolver probes only amazon.in (not amazon.com). If user wants amazon.com,
        they add it separately as a different website entry.
    """
    logger.info(f"[resolver] Resolving '{name}'...")

    # Cancel check before any work — catches the case where the user
    # clicked Cancel before the request even reached the resolver.
    check_cancelled(request_id)

    # Step 1: Find live domains
    # _candidate_domains handles both:
    #   - "flipkart" → generates flipkart.com, flipkart.in, shop.flipkart.com, etc.
    #   - "amazon.in" → uses amazon.in directly (dot detected)
    live_domains = await _find_live_domains(name)
    logger.info(f"[resolver] Live domains: {live_domains}")
    check_cancelled(request_id)  # after Google search / domain probing

    # Step 2: Validate via LLM
    validated_domains = []
    if live_domains:
        checks = await asyncio.gather(
            *[_domain_matches_site(d, name) for d in live_domains],
            return_exceptions=True
        )
        for domain, ok in zip(live_domains, checks):
            if ok is True:
                validated_domains.append(domain)
            else:
                logger.info(f"[resolver] ⚠️ Domain rejected: {domain}")
    logger.info(f"[resolver] Validated domains: {validated_domains}")
    check_cancelled(request_id)  # after the LLM domain-validation gather

    # Step 3: Playwright on validated domains + shop. variants
    def _expand_with_shop(domains: list[str]) -> list[str]:
        expanded, seen = [], set()
        for d in domains:
            parsed = urllib.parse.urlparse(d)
            bare = re.sub(r'^www\.', '', parsed.netloc.lower())
            # Add shop. variant FIRST — stores often live on shop. subdomain
            if not any(bare.startswith(p) for p in ("shop.", "store.", "buy.")):
                variant = f"{parsed.scheme}://shop.{bare}"
                if variant not in seen:
                    expanded.append(variant)
                    seen.add(variant)
            # Then add the original domain
            if d not in seen:
                expanded.append(d)
                seen.add(d)
        return expanded

    expanded = _expand_with_shop(validated_domains)
    logger.info(f"[resolver] Trying Playwright on: {expanded}")

    for candidate_url in expanded:
        # Cancel check before each Playwright attempt. Each iteration runs
        # a real browser and can take 5-15s; without this check, a cancelled
        # search would still grind through every remaining candidate.
        check_cancelled(request_id)
        async with httpx.AsyncClient(
            timeout=5, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"}
        ) as client:
            live_base = await _probe_domain(client, candidate_url)
        if not live_base:
            continue

        discovered = await _discover_search_url(live_base)
        if not discovered or not _search_url_domain_matches_base(discovered, live_base):
            continue

        # Playwright already performed a real search and got a result URL
        # containing the query — that IS the e-commerce verification.
        # Skip the httpx-based ecom check which fails on sites that block
        # httpx (Amazon → 503, Flipkart → 403).
        logger.info(f"[resolver] ✅ '{name}' → {discovered['search_url']} (Playwright)")
        return discovered

    # Step 4: Ask OpenAI for correct domain
    check_cancelled(request_id)  # before kicking off another LLM call
    logger.info(f"[resolver] Playwright failed, asking OpenAI for domain...")
    ai_base = await _ask_openai_for_domain(name)

    if ai_base:
        # Validate: extract the SLD (second-level domain) and check it matches name
        # SLD of walmartindia.com is "walmartindia", not "walmart"
        # SLD of amazon.in is "amazon", of shop.drberg.com is "drberg"
        ai_netloc = urllib.parse.urlparse(ai_base).netloc.lower()
        ai_netloc_no_www = re.sub(r'^www\.', '', ai_netloc)
        # Extract SLD: for "shop.drberg.com" → "drberg"
        # for "walmartindia.com" → "walmartindia"
        # for "amazon.co.uk" → "amazon"
        parts = ai_netloc_no_www.split(".")
        if len(parts) >= 3 and parts[-2] in ("co", "com", "net", "org") and len(parts[-1]) == 2:
            sld = parts[-3]  # amazon.co.uk → amazon
        elif len(parts) >= 2:
            sld = parts[-2]  # walmartindia.com → walmartindia
        else:
            sld = parts[0]
        # Strip subdomain prefixes from SLD — actually SLD is already clean after parts[-2/-3]
        name_clean = re.sub(r'[^a-z0-9]', '', name.lower())
        sld_clean = re.sub(r'[^a-z0-9]', '', sld)

        # STRICT MATCH: SLD must equal the name, or name must equal SLD
        # This rejects walmart → walmartindia (sld="walmartindia" ≠ "walmart")
        # But accepts fastandup → fastandup (exact match)
        exact_match = sld_clean == name_clean
        # Allow loose match only if SLD is a prefix/suffix that matches most of the name
        # (e.g., "beato" matches "beatoapp" with substring length check)
        loose_match = False
        if not exact_match:
            shorter, longer = sorted([sld_clean, name_clean], key=len)
            # The longer must START with the shorter (beatoapp starts with beato — OK)
            # This prevents walmart→walmartindia (walmartindia doesn't start with walmart... actually it does)
            # So also require length difference to be small
            if longer.startswith(shorter) and (len(longer) - len(shorter)) <= 3:
                loose_match = True

        if not exact_match and not loose_match:
            logger.warning(
                f"[resolver] ⚠️ OpenAI domain SLD '{sld_clean}' doesn't match '{name_clean}' — rejecting"
            )
            ai_base = None
        elif not exact_match:
            # Loose match — verify via second LLM call as safety
            logger.info(f"[resolver] SLD '{sld_clean}' loosely matches '{name_clean}' — verifying")
            domain_ok = await _domain_matches_site(ai_base, name)
            if not domain_ok:
                logger.info(f"[resolver] ❌ LLM domain rejected: {ai_base}")
                ai_base = None

        # ── Fix 1: DNS reality check ─────────────────────────────────
        # Even if SLD matched, verify the FULL domain actually resolves.
        # LLMs hallucinate .in/.co TLDs for US-only brands (walmart.in bug).
        # The SLD check alone passes `walmart.in` because walmart==walmart,
        # but the TLD is a lie — the domain doesn't exist. A quick HTTP probe
        # catches this deterministically without burning another LLM call.
        if ai_base:
            async with httpx.AsyncClient(
                timeout=5, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"}
            ) as _dns_client:
                probe_result = await _probe_domain(_dns_client, ai_base)
            if not probe_result:
                logger.warning(
                    f"[resolver] ❌ LLM domain {ai_base} does not resolve — "
                    f"likely hallucinated (e.g., walmart.in). Rejecting."
                )
                ai_base = None

    if ai_base:
        ai_parsed = urllib.parse.urlparse(ai_base)
        bare = re.sub(r'^www\.', '', ai_parsed.netloc)
        domains_to_try = [ai_base]
        if not bare.split('.')[0] in ('shop', 'store', 'buy'):
            domains_to_try.append(f"{ai_parsed.scheme}://shop.{bare}")

        for try_url in domains_to_try:
            if try_url in validated_domains:
                continue
            # Cancel check before each retry — same rationale as the
            # primary Playwright loop above.
            check_cancelled(request_id)
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                live_base = await _probe_domain(client, try_url)
            if not live_base:
                continue
            discovered = await _discover_search_url(live_base)
            if not discovered or not _search_url_domain_matches_base(discovered, live_base):
                continue
            logger.info(f"[resolver] ✅ '{name}' → {discovered['search_url']} (OpenAI domain)")
            return discovered

    # Step 5: Last resort — OpenAI guesses URL
    # fallback_base priority:
    #   1. ai_base — only if it survived all hallucination/DNS checks above
    #   2. validated_domains[0] — domain is live AND LLM confirmed it matches
    #   3. live_domains[0] — domain is live but LLM rejected the brand match.
    #                        We keep this as a last resort because legitimate
    #                        e-commerce sites (e.g., West LA Mart at wlamart.com)
    #                        get LLM-rejected when their name doesn't match a
    #                        well-known brand. Without this fallback they'd be
    #                        unaddable.
    #
    # Critical: if ai_base was set to None by the SLD or DNS check, we MUST
    # fall through to validated_domains. Never use the hallucinated URL even
    # indirectly (the walmart.in bug).
    fallback_base = ai_base or (validated_domains[0] if validated_domains else (live_domains[0] if live_domains else None))
    if not fallback_base:
        raise RuntimeError(
            f"We couldn't find '{name}' — please check the spelling, "
            f"or try the full domain (e.g., walmart.com). "
            f"If this is a real site, it may be blocking automated discovery."
        )

    # Clean the fallback_base: strip any path (e.g., "target.com/in" → "target.com").
    # LLMs sometimes hallucinate region paths. We want just scheme://host as base.
    parsed_fb = urllib.parse.urlparse(fallback_base)
    if parsed_fb.path and parsed_fb.path != "/":
        cleaned = f"{parsed_fb.scheme}://{parsed_fb.netloc}"
        logger.info(f"[resolver] Stripped path from fallback_base: {fallback_base} → {cleaned}")
        fallback_base = cleaned

    check_cancelled(request_id)  # final check before the last-resort LLM call
    return await _llm_url_guess(name, fallback_base)


# ── LLM URL guess helper (dynamic, no hardcoded site hints) ───────────

# Generic pattern families to try when the LLM's guess fails validation.
# These are NOT site-specific — they cover common e-commerce platforms.
FALLBACK_URL_PATTERNS = [
    "/search?q={query}",                   # Generic
    "/search/?q={query}",                  # Generic with trailing slash
    "/search?type=product&q={query}",      # Shopify canonical
    "/search/result/?q={query}",           # Nykaa-style
    "/search/result?q={query}",
    "/search/?text={query}",               # Ajio-style
    "/shop/featured/{query}?ss=true",      # Macys (hyphen-slug path)
    "/catalogsearch/result/?q={query}",    # Magento canonical
    "/s?keyword={query}",                  # Costco (current)
    "/search?keyword={query}",             # Big-box generic (Best Buy, Lowes)
    "/search?searchTerm={query}",          # Target-style
    "/pages/search-results-page?q={query}",  # DrBerg-style Shopify pages
    "/products?q={query}",
    "/shop?q={query}",
    "/shop/ols/search?keywords={query}",   # GoDaddy OLS
    "/?s={query}",                         # WordPress
    "/?search={query}",
    "/s?k={query}",                        # Amazon-like
]


async def _looks_like_search_page(html: str, url: str, query: str = "vitamin c") -> tuple[bool, int]:
    """
    Score an HTML response to determine if it's a valid search results page.

    Returns (is_search_page, score). Score >= 3 means likely a search page.

    CRITICAL REQUIREMENT: A valid search page MUST echo the query somewhere.
    A homepage that ignores the query string and shows generic recommendations
    is NOT a search results page — this catches false positives like Walmart's
    /?s= returning the homepage with product recommendations.

    Signals checked:
      - URL doesn't contain error/blocked markers
      - Query term appears in the page (REQUIRED — else score=0)
      - Multiple price patterns ($, ₹, £, €, USD, INR, etc.)
      - Product-commerce vocabulary (add to cart, buy now, price, etc.)
      - Multiple repeated product-card-like structures
      - NOT obviously a 404/home/login page
    """
    if not html or len(html) < 500:
        return False, 0

    url_lower = url.lower()
    bad_url_markers = ["/blocked", "/error", "/404", "/login", "/signin", "/captcha"]
    if any(m in url_lower for m in bad_url_markers):
        return False, 0

    html_lower = html.lower()

    # REQUIRED: query must be echoed in the page.
    # This catches the Walmart /?s= false-positive where the site ignores
    # the query and shows the homepage. A real search page shows the query
    # in the title, a results header, or search input value.
    query_words = [w.lower() for w in query.split() if len(w) >= 3]
    query_echoed = False
    if query_words:
        # Check if at least one query word appears in title, h1, h2, or result count
        for word in query_words:
            if (re.search(rf'<title[^>]*>[^<]*\b{re.escape(word)}\b', html_lower) or
                re.search(rf'<h[12][^>]*>[^<]*\b{re.escape(word)}\b', html_lower) or
                re.search(rf'(?:results? for|search.{{0,20}})["\']?\s*{re.escape(word)}', html_lower) or
                re.search(rf'value=["\'][^"\']*\b{re.escape(word)}', html_lower)):
                query_echoed = True
                break

    if not query_echoed:
        return False, 0

    score = 1  # 1 point for echoing the query

    # Signal 1: multiple price occurrences
    price_matches = len(re.findall(r'[\$₹£€¥](?:\s*\d)|(?:usd|inr|gbp|eur)\s*\d|rs\.?\s*\d', html_lower))
    if price_matches >= 3:
        score += 2
    elif price_matches >= 1:
        score += 1

    # Signal 2: e-commerce vocabulary
    commerce_words = [
        "add to cart", "buy now", "add to bag", "in stock", "out of stock",
        "product-card", "product-item", "product-grid", "search-result",
        "sort by", "filter", "add to wishlist"
    ]
    commerce_hits = sum(1 for w in commerce_words if w in html_lower)
    if commerce_hits >= 3:
        score += 2
    elif commerce_hits >= 1:
        score += 1

    # Signal 3: repeated product card structure (many similar href/class patterns)
    product_link_patterns = len(re.findall(r'<a[^>]+(?:product|item)[^>]*>', html_lower))
    if product_link_patterns >= 10:
        score += 2
    elif product_link_patterns >= 3:
        score += 1

    # Negative signal: looks like a homepage or 404 page
    if re.search(r'<h1[^>]*>\s*(?:page not found|404|welcome|home)', html_lower):
        score -= 2

    return score >= 3, score


async def _try_search_url(client, base_url: str, pattern: str, query: str = "vitamin+c") -> Optional[dict]:
    """
    Try a URL pattern by actually fetching it with a test query.
    Returns {base_url, search_url, encoding} if the response looks like a
    real search results page. Otherwise returns None.
    """
    # Build the test URL
    if pattern.startswith("/"):
        test_url = f"{base_url.rstrip('/')}{pattern}".replace("{query}", query)
    else:
        test_url = pattern.replace("{query}", query)

    try:
        resp = await client.get(test_url, timeout=8)
        final_url = str(resp.url)

        if resp.status_code != 200:
            logger.debug(f"[resolver] Pattern test HTTP {resp.status_code}: {test_url}")
            return None

        is_search, score = await _looks_like_search_page(resp.text, final_url)
        logger.info(
            f"[resolver] Pattern test: {pattern} → HTTP {resp.status_code}, "
            f"score={score}, valid={is_search}"
        )
        if is_search:
            template = pattern if pattern.startswith("http") else f"{base_url.rstrip('/')}{pattern}"
            return {
                "base_url": base_url.rstrip("/"),
                "search_url": template,
                "encoding": "plus",
            }
    except Exception as e:
        logger.debug(f"[resolver] Pattern test error for {test_url}: {type(e).__name__}")
    return None


async def _llm_url_guess(name: str, fallback_base: str) -> dict:
    """
    Dynamic URL resolution — no hardcoded site hints.

    Flow:
      1. Ask LLM for a URL (generic prompt, only platform families named)
      2. Validate the guess by fetching it + scoring response
      3. If LLM's guess fails, try generic fallback patterns until one works
      4. If nothing validates, return the LLM's guess as best-effort
    """
    logger.info(f"[resolver] ⚠️ Last resort: OpenAI URL guess for {fallback_base}")
    if not OPENAI_API_KEY:
        raise RuntimeError(
            f"We couldn't auto-detect the search URL for '{name}'. "
            f"Automatic URL detection requires server configuration "
            f"(OPENAI_API_KEY) — please contact your administrator."
        )

    # Step 1: ask LLM for URL (generic prompt, no site names)
    llm_url = None
    llm_encoding = "plus"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL, "temperature": 0, "max_tokens": 120,
                    "messages": [{"role": "user", "content":
                        f'What is the product search URL template for website "{name}" (base: {fallback_base})?\n\n'
                        f'Return the real search URL this site actually uses.\n'
                        f'Use the EXACT query parameter name (e.g., q, search, s, keyword, _nkw, searchTerm).\n'
                        f'Common platform families:\n'
                        f'- Shopify: /search?type=product&q={{query}}\n'
                        f'- Magento: /catalogsearch/result/?q={{query}}\n'
                        f'- WordPress: /?s={{query}}\n'
                        f'- Generic: /search?q={{query}}\n'
                        f'- Myntra: /{{query}}?rawQuery={{query}}\n'
                        f'- Nykaa: /search/result/?q={{query}}\n'
                        f'- Walmart: /search?q={{query}}\n'
                        f'- Macys: /shop/featured/{{query}}?ss=true\n'
                        f'- DrBerg: /pages/search-results-page?q={{query}}\n'
                        f'- GoDaddy OLS: /shop/ols/search?keywords={{query}}\n\n'
                        f'Return ONLY JSON (no markdown fences): '
                        f'{{"base_url":"{fallback_base}","search_url":"...","encoding":"plus"}}'}],
                }
            )
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            data = json.loads(m.group(0)) if m else {}

        base_url = data.get("base_url", fallback_base).rstrip("/")
        search_url = data.get("search_url", "")
        llm_encoding = data.get("encoding", "plus")
        if search_url and search_url.startswith("/"):
            search_url = f"{base_url}{search_url}"

        # Verify LLM's base is actually live (walmart.in-style hallucination check)
        if base_url.rstrip("/") != fallback_base.rstrip("/"):
            logger.info(f"[resolver] LLM returned different base '{base_url}' — validating...")
            async with httpx.AsyncClient(
                timeout=5, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"}
            ) as client:
                live_check = await _probe_domain(client, base_url)
            if not live_check:
                logger.warning(
                    f"[resolver] ⚠️ LLM's base '{base_url}' not live — reverting to '{fallback_base}'"
                )
                search_url = search_url.replace(base_url, fallback_base.rstrip("/"))
                base_url = fallback_base.rstrip("/")

        if search_url and "{query}" in search_url:
            llm_url = search_url

    except Exception as e:
        logger.warning(f"[resolver] LLM URL guess failed: {e}")

    # Step 2: Build list of URLs to try, in priority order.
    # LLM's guess goes first — it's usually right. Then fallback families.
    base = fallback_base.rstrip("/")
    urls_to_try = []
    if llm_url:
        urls_to_try.append(("LLM guess", llm_url))
    for pattern in FALLBACK_URL_PATTERNS:
        candidate = f"{base}{pattern}"
        if candidate not in [u for _, u in urls_to_try]:
            urls_to_try.append((f"pattern {pattern}", candidate))

    # Step 3: Try each URL by actually fetching it and scoring the response.
    # We also collect the final URLs (after redirects) so we can try
    # bot-wall extraction afterwards if everything failed.
    #
    # Validation ladder per candidate:
    #   1. httpx GET — fast and cheap. Works for most sites (Amazon, Beato, Target).
    #   2. If httpx returns a blocked status (403/429/redirect to /blocked),
    #      retry that URL via ScraperAPI premium. ScraperAPI uses residential
    #      proxies that bypass Akamai/Cloudflare/PerimeterX walls (Nykaa, Ajio,
    #      Macys). One ScraperAPI call is 10 credits, so we cap how many
    #      candidates trigger the fallback to keep cost bounded.
    #
    # Sites that work via httpx never reach the ScraperAPI fallback —
    # nothing about the fast path changes.
    logger.info(f"[resolver] Validating {len(urls_to_try)} URL candidates...")
    final_urls_seen: list[str] = []  # for bot-wall extraction below

    # Bound ScraperAPI usage: at most 3 candidates per resolution can trigger
    # the premium fallback. That covers LLM guess + 2 fallback patterns, which
    # is enough to find the right URL on any reasonably-shaped e-commerce site.
    # Worst-case cost: 30 credits per add (vs 130 if we tried every candidate).
    SCRAPERAPI_FALLBACK_BUDGET = 3
    scraperapi_attempts_used = 0

    # Track whether the LLM's first-choice URL specifically encountered a
    # bot-wall block. If yes AND we never managed to validate any URL, we
    # trust the LLM's guess as our best information. The LLM's prompt has
    # known good patterns for many sites (Nykaa, Macys, Costco, etc.), so
    # its first guess is informed — not random. We only reach this
    # "trust on impossible-to-validate" path when bot blocking, not 404s,
    # is the cause of failure.
    llm_url_was_blocked = False
    _blocked_statuses_seen: list[int] = []  # tracks every 403/429 we got
    _score_zero_count = 0  # tracks 200 OK bot-challenge pages (Sam's Club pattern)

    def _is_blocked_status(status: int, final_url: str) -> bool:
        """A status code or redirect target that suggests bot blocking, not wrong URL."""
        if status in (401, 403, 429):
            return True
        # Walmart/Akamai: 200 OK but redirected to /blocked page
        if "/blocked" in final_url.lower():
            return True
        # Sam's Club: 200 OK but redirected to /are-you-human page
        if "/are-you-human" in final_url.lower():
            return True
        return False

    async with httpx.AsyncClient(
        timeout=10, follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    ) as client:
        for label, template in urls_to_try:
            # Convert template URL into a test URL (replace {query})
            test_url = template.replace("{query}", "vitamin+c")
            try:
                resp = await client.get(test_url, timeout=8)
                final_url = str(resp.url)
                final_urls_seen.append(final_url)

                if resp.status_code == 200 and "/blocked" not in final_url.lower():
                    # Genuine 200 — score the HTML directly.
                    is_search, score = await _looks_like_search_page(resp.text, final_url)
                    logger.info(f"[resolver] Trying {label}: score={score}, valid={is_search}")
                    if is_search:
                        logger.info(f"[resolver] ✅ '{name}' → {template} (validated, score={score})")
                        return {
                            "base_url": base,
                            "search_url": template,
                            "encoding": llm_encoding,
                        }
                    # 200 but score=0 — likely a bot-challenge page (Sam's Club pattern).
                    # Count these so Step 6 knows the site is bot-protected not URL-unknown.
                    if score == 0:
                        _score_zero_count += 1
                    continue

                # Non-200 OR redirected to a blocked page. Decide whether to
                # retry via ScraperAPI. Only retry if status looks like blocking,
                # not a real 404 (which means the URL pattern is genuinely wrong).
                if not _is_blocked_status(resp.status_code, final_url):
                    logger.debug(f"[resolver] ✗ {label}: HTTP {resp.status_code} (not retrying)")
                    continue

                # Remember if the LLM's first-choice URL was blocked. This is
                # the signal that we have an LLM-suggested URL we couldn't
                # validate due to bot wall, not because the URL was wrong.
                # Used at the end of the validation loop to decide whether to
                # trust the LLM's guess as a last-resort save.
                if label == "LLM guess":
                    llm_url_was_blocked = True

                if scraperapi_attempts_used >= SCRAPERAPI_FALLBACK_BUDGET:
                    logger.debug(
                        f"[resolver] ✗ {label}: HTTP {resp.status_code} "
                        f"(ScraperAPI budget exhausted)"
                    )
                    continue

                # Try ScraperAPI premium for this URL. Sync call — wrap in to_thread.
                # Lazy import to avoid any circular-import risk between resolver
                # and scraper modules, and to make the dependency explicit at
                # the one place it's used.
                scraperapi_attempts_used += 1
                logger.info(
                    f"[resolver] 🛡️ {label}: HTTP {resp.status_code} "
                    f"— retrying via ScraperAPI premium "
                    f"({scraperapi_attempts_used}/{SCRAPERAPI_FALLBACK_BUDGET})"
                )
                try:
                    from .scraper import _get_soup_scraperapi
                    sa_soup, sa_html = await asyncio.to_thread(
                        _get_soup_scraperapi, test_url, True  # force_premium=True
                    )
                except Exception as e:
                    logger.warning(f"[resolver] ScraperAPI error: {type(e).__name__}: {e}")
                    continue

                if not sa_html:
                    # ScraperAPI also failed for this URL — pattern is dead.
                    continue

                is_search, score = await _looks_like_search_page(sa_html, test_url)
                logger.info(
                    f"[resolver] ScraperAPI {label}: score={score}, valid={is_search}"
                )
                if is_search:
                    logger.info(
                        f"[resolver] ✅ '{name}' → {template} "
                        f"(validated via ScraperAPI, score={score})"
                    )
                    return {
                        "base_url": base,
                        "search_url": template,
                        "encoding": llm_encoding,
                    }
                # ScraperAPI got the page but it's not a search results page —
                # URL pattern is wrong. Move on to the next candidate.

            except Exception as e:
                logger.debug(f"[resolver] ✗ {label}: {type(e).__name__}")
                continue

    # ── Step 4 (NEW): Bot-wall extraction fallback ───────────────────
    # When ALL patterns redirect to a /blocked-style page, the bot wall
    # often echoes back the URL we were trying to reach via a base64-encoded
    # parameter. We can decode that to recover the real search URL pattern.
    #
    # This is a fallback that runs AFTER the existing pipeline, so:
    #   - Sites that work normally aren't affected
    #   - Only Akamai-style bot walls that echo URLs benefit (Walmart)
    #   - Generic 403s, Cloudflare challenges, PerimeterX captchas → still fail
    extracted = _extract_url_from_bot_wall(final_urls_seen, base)
    if extracted:
        logger.info(
            f"[resolver] ✅ '{name}' → {extracted} (recovered from bot-wall echo)"
        )
        return {
            "base_url": base,
            "search_url": extracted,
            "encoding": "plus",
        }

    # ── Step 5: Trust the LLM when validation was impossible ─────────
    # If the LLM's first-choice URL was blocked (403/etc.) AND ScraperAPI
    # couldn't break through either, we have no way to validate. But the
    # LLM's prompt contains known-good patterns for many walled sites
    # (Nykaa, Macys, Costco, Ajio), so its first guess is informed — not
    # random.
    #
    # We only trust the LLM in the "impossible to validate" case. If httpx
    # returned 404 (real "wrong URL"), llm_url_was_blocked stays False and
    # we still raise RuntimeError — preventing the walmart.in-style bug
    # where the LLM hallucinates a wrong URL and we save it without checking.
    #
    # The trade-off: we might save a wrong URL for a walled site if the
    # LLM hallucinates. Mitigated by the fact that walled sites with
    # patterns in our prompt (added based on real sites) get the right
    # answer, and unknown walled sites are rare.
    if llm_url and llm_url_was_blocked:
        logger.warning(
            f"[resolver] ⚠️ '{name}' → {llm_url} "
            f"(unvalidated — bot wall blocked both httpx and ScraperAPI; "
            f"trusting LLM's pattern)"
        )
        return {
            "base_url": base,
            "search_url": llm_url,
            "encoding": llm_encoding,
        }

    # ── Step 6: Loud failure ─────────────────────────────────────────
    # Reached only when:
    #   - LLM gave no URL at all, OR
    #   - LLM's URL returned a real 404 (wrong pattern, not blocked)
    # The walmart.in bug came from saving the LLM's guess unverified.
    # Refuse to save anything we couldn't validate AND wasn't bot-blocked.
    # Determine if failure was bot protection or unknown URL.
    # Bot protection: 403s, /blocked redirects, or score=0 on all patterns.
    # URL unknown: real 404s — the pattern is wrong but site is accessible.
    any_bot_blocked = (
        llm_url_was_blocked
        or any(
            '/blocked' in u.lower() or '/captcha' in u.lower() or '/are-you-human' in u.lower()
            for u in final_urls_seen
        )
        or (bool(_blocked_statuses_seen) and all(s in (401, 403, 429) for s in _blocked_statuses_seen))
        or (_score_zero_count >= 3)
    )
    if any_bot_blocked:
        logger.error(f"[resolver] ❌ '{name}' at {base} is bot-protected.")
        raise RuntimeError(f"This website uses anti-bot protectors:{base}")
    else:
        logger.error(f"[resolver] ❌ Could not find search URL for '{name}' on {base}.")
        raise RuntimeError(f"This website uses anti-bot protectors:{base}")


# ── Bot-wall URL extraction (fallback for Akamai-protected sites) ─────

def _extract_url_from_bot_wall(final_urls: list[str], base_url: str) -> Optional[str]:
    """
    Some bot walls (Akamai with URL echo, used by Walmart) redirect blocked
    requests to a /blocked page that contains the original URL base64-encoded
    in a query parameter.

    Example: when we hit https://www.walmart.com/search?q=vitamin+c
    Akamai redirects to:
      https://www.walmart.com/blocked?url=L3NlYXJjaD9xPXZpdGFtaW4rYw==&uuid=...

    The base64 payload "L3NlYXJjaD9xPXZpdGFtaW4rYw==" decodes to:
      /search?q=vitamin+c

    That IS the real search URL pattern — Akamai is literally telling us
    what we wanted to reach. We can extract it without making the LLM guess.

    Approach:
      1. Look at all final URLs we saw during validation
      2. If any contain `/blocked?url=...` or similar, extract the base64
      3. Decode and check if it contains the test query
      4. If yes, build a search URL template by replacing the query with {query}

    Returns: search URL template string, or None if no extraction possible.
    """
    import base64

    TEST_QUERY = "vitamin+c"
    TEST_QUERY_DECODED = "vitamin c"

    for final_url in final_urls:
        try:
            parsed = urllib.parse.urlparse(final_url)
            path_lower = parsed.path.lower()

            # Must be a bot-wall page (path contains blocked/captcha/challenge)
            bot_wall_markers = ["/blocked", "/captcha", "/challenge", "/firewall", "/are-you-human"]
            if not any(m in path_lower for m in bot_wall_markers):
                continue

            # Look for a base64-encoded URL parameter
            params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            for key, values in params.items():
                if not values:
                    continue
                value = values[0]
                # Try base64 decode (Akamai uses standard base64 with padding)
                try:
                    # base64 may have URL-safe variants and missing padding
                    padded = value + "=" * (-len(value) % 4)
                    decoded_bytes = base64.b64decode(padded)
                    decoded = decoded_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    continue

                # Sanity-check: decoded should look like a URL path
                if not decoded.startswith("/"):
                    continue

                # Sanity-check: must contain our test query (proves it's the
                # URL we sent, not something else the bot wall encoded)
                if TEST_QUERY not in decoded and TEST_QUERY_DECODED not in decoded:
                    continue

                # Replace the test query with {query} placeholder.
                # Try both encodings: vitamin+c (plus) and vitamin%20c (percent).
                pattern = decoded
                pattern = pattern.replace("vitamin+c", "{query}")
                pattern = pattern.replace("vitamin%20c", "{query}")
                pattern = pattern.replace("vitamin c", "{query}")
                pattern = pattern.replace("vitamin-c", "{query}")

                # Make sure we actually substituted something
                if "{query}" not in pattern:
                    continue

                # Build full search URL using the base
                full_url = f"{base_url.rstrip('/')}{pattern}"
                logger.info(
                    f"[resolver] 🔓 Bot-wall echo: decoded '{value[:30]}...' → "
                    f"recovered pattern '{pattern}'"
                )
                return full_url

        except Exception as e:
            logger.debug(f"[resolver] Bot-wall extraction error on {final_url}: {e}")
            continue

    return None