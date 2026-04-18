"""
scout_agent/tools.py — LangChain tools wrapping Scout's core capabilities.

Each tool is a thin, well-typed wrapper around the existing FastAPI
backend logic so the agent can call them without going through HTTP.
Tools are kept synchronous by running coroutines with asyncio.run()
so LangChain's OPENAI_FUNCTIONS agent (which is sync) works out of
the box. Swap to async tools + AsyncCallbackManager if you later move
to an async agent runner.
"""

import asyncio
import json
import logging
from typing import Optional



logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _run(coro):
    """Run a coroutine from sync context, reusing the running loop if possible."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ──────────────────────────────────────────────────────────────────────
# Tool 1 — search_products
# ──────────────────────────────────────────────────────────────────────

def search_products(query: str) -> str:
    """
    Search for a product across all active e-commerce platforms (Amazon,
    Flipkart, Myntra, Nykaa, Beato, etc.).

    Input : product name — can be short ("dyson hair dryer") or a full
            product title. Brand name in the query is respected.
    Output: JSON string with a list of listings, each containing platform,
            title, price (value + currency), URL, and availability.

    Use this as the FIRST step when the user asks about prices, availability,
    or wants to compare a product.
    """
    from scout.scout_db import db
    from scout.router import _search_across_sites

    platforms = [s["name"] for s in db.get_active_websites()]
    result = _run(_search_across_sites(query.strip(), platforms))

    listings = result.get("listings", [])
    platform_status = result.get("platform_status", {})

    summary = {
        "query": query,
        "total_listings": len(listings),
        "platform_status": platform_status,
        "listings": [
            {
                "platform": l.get("platform"),
                "title": l.get("title"),
                "price": l.get("price", {}).get("value"),
                "currency": l.get("price", {}).get("currency", "INR"),
                "url": l.get("url"),
                "availability": l.get("availability", "unknown"),
            }
            for l in listings
        ],
    }
    return json.dumps(summary, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Tool 2 — compare_prices
# ──────────────────────────────────────────────────────────────────────

def compare_prices(query: str) -> str:
    """
    Compare prices for a product across platforms after entity resolution.
    Groups listings that are the *same physical product* and surfaces the
    cheapest platform, price spread, and potential savings.

    Input : product name (same format as search_products).
    Output: JSON with comparable entities (found on 2+ platforms) and
            single-platform results, including best_savings percentage.

    Use this when the user asks "where is X cheapest?" or "which platform
    has the best price for Y?". Run search_products first if no data exists.
    """
    from scout.scout_db import db
    from scout.entity_resolver import get_entities_for_query

    entities = get_entities_for_query(db, query.strip())
    comparable = [e for e in entities if e.get("platform_count", 0) >= 2]
    single = [e for e in entities if e.get("platform_count", 0) < 2]

    result = {
        "query": query,
        "comparable_products": [
            {
                "product": e["canonical_name"],
                "brand": e.get("canonical_brand"),
                "cheapest_platform": e["best_price"].get("platform") if e.get("best_price") else None,
                "cheapest_price": e["best_price"].get("price") if e.get("best_price") else None,
                "price_spread": e.get("price_spread"),
                "platforms": [
                    {"platform": l["platform"], "price": l["price"]["value"]}
                    for l in sorted(e.get("listings", []), key=lambda x: x["price"]["value"])
                ],
            }
            for e in comparable
        ],
        "single_platform_products": [
            {
                "product": e["canonical_name"],
                "platform": e["listings"][0]["platform"] if e.get("listings") else None,
                "price": e["best_price"].get("price") if e.get("best_price") else None,
            }
            for e in single
        ],
        "summary": {
            "total_entities": len(entities),
            "cross_platform_count": len(comparable),
            "best_savings_percent": max(
                (e["price_spread"]["diff_percent"] for e in comparable if e.get("price_spread")),
                default=0,
            ),
        },
    }
    return json.dumps(result, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Tool 3 — get_features
# ──────────────────────────────────────────────────────────────────────

def get_features(query: str) -> str:
    """
    Extract and compare product features/specs across platforms for a query.
    Detects category automatically (health, electronics, fashion, grocery,
    beauty) and returns a feature matrix showing which platforms agree or
    differ on each spec.

    Input : product name.
    Output: JSON with category, feature_matrix (rows = features,
            cols = platforms), and per-platform details including
            seller, rating, return policy, and shipping info.

    Use this when the user asks "what are the specs of X?", "does platform
    A and B sell the same variant?", or wants a detailed comparison table.
    """
    from scout.scout_db import db
    from scout.entity_resolver import get_entities_for_query
    from scout.feature_extractor import compare_features
    import json as _json

    entities = get_entities_for_query(db, query.strip())
    if not entities:
        return json.dumps({
            "error": f"No data found for '{query}'. Try running search_products first.",
            "query": query,
        })

    entity = entities[0]
    listings = entity.get("listings", [])

    enriched = []
    for listing in listings:
        platform = listing["platform"]
        cached = db.get_cached_features(query, platform)
        if cached:
            listing["_cached_features"] = cached
            listing["product_details"] = {}
        else:
            with db._conn() as conn:
                row = db._fetchone(conn, """
                    SELECT product_details FROM product_results
                    WHERE product_name = %s AND platform = %s
                    ORDER BY scraped_at DESC LIMIT 1
                """, (query, platform))
            if row:
                details = row.get("product_details") or {}
                if isinstance(details, str):
                    try:
                        details = _json.loads(details)
                    except Exception:
                        details = {}
                listing["product_details"] = details
        enriched.append(listing)

    result = compare_features(query, enriched)
    return json.dumps(result, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Tool 4 — get_price_history
# ──────────────────────────────────────────────────────────────────────

def get_price_history(query: str) -> str:
    """
    Retrieve historical price data for a product across platforms.
    Returns time-series data with a trend summary (up / down / stable).

    Input : product name.
    Output: JSON with per-platform trend (latest_price, min_price,
            max_price, trend direction) and full raw_history array.

    Use this when the user asks "has the price of X gone up or down?",
    "what was the price last week?", or "should I buy now or wait?".
    """
    from scout.scout_db import db

    history = db.get_price_history(query.strip(), platform=None, limit=90)

    grouped: dict = {}
    for row in history:
        p = row["platform"]
        grouped.setdefault(p, []).append({
            "price": float(row["price"]),
            "currency": row.get("currency", "INR"),
            "scraped_at": row["scraped_at"].isoformat() if row.get("scraped_at") else None,
        })

    if not grouped:
        return json.dumps({
            "error": f"No price history for '{query}'. Run search_products first.",
            "query": query,
        })

    trends = {}
    for platform, points in grouped.items():
        prices = [p["price"] for p in points]
        trends[platform] = {
            "latest_price": prices[-1],
            "oldest_price": prices[0],
            "min_price": min(prices),
            "max_price": max(prices),
            "data_points": len(prices),
            "trend": "down" if prices[-1] < prices[0] else "up" if prices[-1] > prices[0] else "stable",
        }

    return json.dumps({
        "query": query,
        "trends": trends,
        "raw_history": grouped,
    }, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Tool 5 — get_alerts
# ──────────────────────────────────────────────────────────────────────

def get_alerts(input: str = "") -> str:
    """
    Retrieve recent price change alerts for all tracked products.
    Alerts fire when prices go up, drop, or a product becomes
    available / unavailable.

    Input : pass "all" to include already-read alerts.
            Pass empty string "" or "unread" for unread only (default).
    Output: JSON with unread_count badge and alert list (product,
            platform, old/new price, change_percent, direction).

    Use this when the user asks "any price drops?", "what changed?",
    or "show me alerts".
    """
    from scout.scout_db import db

    # LangChain passes a string — parse it
    include_acknowledged = input.strip().lower() in ("all", "true", "yes", "acknowledged")

    alerts = db.get_alerts(unacknowledged_only=not include_acknowledged, limit=50)
    unread_count = db.get_unacknowledged_count()

    return json.dumps({
        "unread_count": unread_count,
        "alerts": [
            {
                "id": a["id"],
                "product": a["product_name"],
                "platform": a["platform"],
                "old_price": float(a["old_price"]) if a.get("old_price") is not None else None,
                "new_price": float(a["new_price"]),
                "change_percent": float(a["change_percent"]) if a.get("change_percent") else None,
                "direction": a["direction"],
                "detected_at": a["detected_at"].isoformat() if a.get("detected_at") else None,
            }
            for a in alerts
        ],
    }, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Tool 6 — run_price_monitor
# ──────────────────────────────────────────────────────────────────────

def run_price_monitor(confirm: str = "yes") -> str:
    """
    Trigger a full price refresh for ALL tracked products across all active
    platforms. Bypasses cache and re-scrapes everything, then generates
    price alerts for any changes detected.

    Input : confirm — must be "yes" to proceed (safety guard against
            accidental heavy scrape runs).
    Output: JSON with products_checked and alerts_generated counts.

    Use this when the user says "refresh prices", "check latest prices",
    or "run price monitor". Confirm intent before running.
    """
    if confirm.strip().lower() != "yes":
        return json.dumps({"error": "Pass confirm='yes' to run the price monitor."})

    from scout.scout_db import db
    from scout.router import _search_across_sites

    rows, _ = db.get_all_products()
    if not rows:
        return json.dumps({"status": "nothing_to_monitor", "products_checked": 0})

    platforms = [s["name"] for s in db.get_active_websites()]
    checked = 0

    for row in rows:
        product_name = row.get("name") or row.get("product_name", "")
        if not product_name:
            continue
        _run(_search_across_sites(product_name, platforms, force_refresh=True))
        checked += 1

    alerts = db.get_alerts(limit=checked * 10)
    return json.dumps({
        "status": "completed",
        "products_checked": checked,
        "alerts_generated": len(alerts),
    }, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Tool 7 — list_platforms
# ──────────────────────────────────────────────────────────────────────

def list_platforms(input: str)  -> str:
    """
    List all active e-commerce platforms Scout can currently search.

    Input : ignored — pass empty string "".
    Output: JSON list of platform names and count.

    Use this when the user asks "which platforms do you support?" or
    before running a targeted search on specific platforms.
    """
    from scout.scout_db import db

    sites = db.get_active_websites()
    return json.dumps({
        "active_platforms": [s["name"] for s in sites],
        "count": len(sites),
    }, indent=2)


# ──────────────────────────────────────────────────────────────────────
# Export — import this list in scout_agent.py
# ──────────────────────────────────────────────────────────────────────

SCOUT_TOOLS = [
    search_products,
    compare_prices,
    get_features,
    get_price_history,
    get_alerts,
    run_price_monitor,
    list_platforms,
]