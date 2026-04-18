"""
scout/router.py — Scout Agent API Routes (extracted from old main.py)

Mounted in main.py with:
    from scout.router import scout_router
    app.include_router(scout_router)
"""

import csv
import io
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from .scout_db import db
from .entity_resolver import (
    get_entities_for_query,
    resolve_entities,
    save_entities,
    extract_brand_from_title,
)
from .feature_extractor import compare_features
from .resolver import resolve_website
from .scraper import scrape_product_on_site
from . import langfuse_config                       # stays at top-level, NOT in scout/

logger = logging.getLogger(__name__)

scout_router = APIRouter(tags=["scout"])

CACHE_TTL_MINUTES = 120
BULK_CONCURRENCY  = 5


# ── Pydantic models ───────────────────────────────────────────────────

class AddWebsiteRequest(BaseModel):
    name: str

class UpdateWebsiteRequest(BaseModel):
    name:       str
    search_url: str
    base_url:   Optional[str] = None
    active:     bool = True

class SearchRequest(BaseModel):
    name:          str
    platforms:     Optional[list[str]] = []
    force_refresh: bool = False


# ── Helpers ───────────────────────────────────────────────────────────

def _shape_site(site: dict) -> dict:
    return {
        "name":       site["name"],
        "search_url": site.get("search_url", ""),
        "base_url":   site.get("base_url", ""),
        "encoding":   site.get("encoding", "plus"),
        "active":     bool(site["active"]),
    }


def _parse_product_names(names_raw: list[str]) -> list[str]:
    seen, result = set(), []
    for name in names_raw:
        name = name.strip()
        if name and name.lower() not in seen:
            seen.add(name.lower())
            result.append(name)
    return result


def _generate_query_variants(query: str) -> list[str]:
    q = query.lower().strip()
    variants = [q]
    words = q.split()
    noise = {"for", "with", "and", "the", "buy", "online"}
    filtered = [w for w in words if w not in noise]
    if filtered:
        variants.append(" ".join(filtered))
    return list(dict.fromkeys(variants))


# ── Core search logic ─────────────────────────────────────────────────

async def _search_across_sites(
    product_name:  str,
    platform_names: list[str],
    force_refresh:  bool = False,
) -> dict:
    sites   = db.get_active_websites()
    targets = [s for s in sites if s["name"] in platform_names]

    if not targets:
        return {"name": product_name, "listings": []}

    if not force_refresh:
        cached_listings:  list[dict] = []
        stale_platforms:  list[dict] = []
        missing_platforms: list[dict] = []

        for site in targets:
            platform = site["name"]
            cached   = db.get_cached_listing(product_name, platform, CACHE_TTL_MINUTES)
            if cached:
                cached_listings.append(cached)
                logger.info(f"[cache] ✅ HIT   {platform} — '{product_name}'")
            else:
                age = db.get_listing_age_minutes(product_name, platform)
                if age is not None:
                    stale_platforms.append(site)
                    logger.info(f"[cache] ⚠️  STALE {platform} — '{product_name}' ({age:.0f}m ago)")
                else:
                    missing_platforms.append(site)
                    logger.info(f"[cache] ❌ MISS  {platform} — '{product_name}' (never scraped)")

        if not stale_platforms and not missing_platforms:
            logger.info(
                f"[cache] Serving '{product_name}' entirely from cache "
                f"({len(cached_listings)} platforms)"
            )
            return {"name": product_name, "listings": cached_listings, "source": "cache"}

        platforms_to_scrape = stale_platforms + missing_platforms
        if cached_listings:
            logger.info(
                f"[cache] Partial hit — {len(cached_listings)} cached, "
                f"{len(platforms_to_scrape)} to scrape"
            )
    else:
        platforms_to_scrape = targets
        cached_listings     = []
        logger.info(f"[cache] force_refresh=True — scraping all {len(targets)} platforms")

    fresh_listings: list[dict] = []

    async def _scrape_one_platform(site: dict) -> Optional[dict]:
        platform_name = site["name"]
        logger.info(f"[search] Trying variants for {platform_name}: '{product_name}'")
        variants = _generate_query_variants(product_name)
        for variant in variants:
            try:
                result = await scrape_product_on_site(site, variant)
                if result and result.get("listings"):
                    best = result["listings"][0]
                    logger.info(f"[search] ✅ {platform_name} worked with: '{variant}'")
                    return best
            except Exception as e:
                logger.error(f"[search] Error on {platform_name} with '{variant}': {e}")
        logger.warning(f"[search] 🚫 {platform_name} failed for ALL variants")
        return None

    results = await asyncio.gather(
        *[_scrape_one_platform(site) for site in platforms_to_scrape],
        return_exceptions=True,
    )

    for i, res in enumerate(results):
        if isinstance(res, Exception):
            logger.error(f"[search] Exception for '{platforms_to_scrape[i]['name']}': {res}")
        elif res is not None:
            fresh_listings.append(res)

    final_product = {
        "name":     product_name,
        "listings": cached_listings + fresh_listings,
    }

    found_platforms  = {l["platform"] for l in final_product["listings"]}
    cached_platforms = {c["platform"] for c in cached_listings}
    final_product["platform_status"] = {
        s["name"]: (
            "found"     if s["name"] in found_platforms
            else "cache" if s["name"] in cached_platforms
            else "not_found"
        )
        for s in targets
    }

    if not fresh_listings:
        return final_product

    db.save_product_result(final_product, platform_names)

    entities = resolve_entities(final_product["listings"], product_name)
    if entities:
        save_entities(db, entities)
        for entity in entities:
            db.link_product_to_entity(
                product_name=product_name,
                entity_id=str(entity["entity_id"])
            )

    for listing in final_product["listings"]:
        price    = listing.get("price", {}).get("value", 0)
        currency = listing.get("price", {}).get("currency", "INR")
        platform = listing.get("platform", "")
        url      = listing.get("url", "")
        if price and platform:
            db.save_price(
                product_name=product_name,
                platform=platform,
                price=price,
                currency=currency,
                url=url or "",
            )

    return final_product


async def _run_bulk_search(
    product_names: list[str],
    platforms:     list[str],
) -> dict:
    semaphore = asyncio.Semaphore(BULK_CONCURRENCY)

    async def _limited(name: str) -> dict:
        async with semaphore:
            try:
                return await _search_across_sites(name, platforms)
            except Exception as exc:
                logger.error(f"Bulk search error for '{name}': {exc}")
                return {"name": name, "listings": []}

    results = await asyncio.gather(
        *[_limited(name) for name in product_names],
        return_exceptions=True,
    )

    final = []
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            logger.error(f"Bulk gather exception for '{product_names[i]}': {res}")
            final.append({"name": product_names[i], "listings": []})
        else:
            final.append(res)

    return {"status": "success", "products": final}


# ══════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════

@scout_router.get("/langfuse/status", tags=["observability"])
def langfuse_status():
    lf = langfuse_config.get_langfuse()
    host = (
        langfuse_config._clean_env("LANGFUSE_HOST")
        or langfuse_config._clean_env("LANGFUSE_BASE_URL")
        or "not set"
    )
    if lf:
        try:
            lf.auth_check()
            return {"status": "active", "host": host,
                    "message": "Langfuse is tracking LLM costs and traces."}
        except Exception as e:
            return {"status": "error", "host": host,
                    "message": f"Langfuse auth failed: {e}"}
    return {"status": "disabled",
            "message": "Set LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY in .env to enable."}


@scout_router.get("/websites")
def get_platforms():
    sites = db.get_active_websites()
    return {"platforms": [s["name"] for s in sites]}


@scout_router.get("/websites/all")
def get_all_websites():
    sites = db.get_all_websites()
    return {"data": [_shape_site(s) for s in sites]}


@scout_router.post("/websites")
async def add_website(payload: AddWebsiteRequest):
    name = payload.name.strip()
    if not name:
        raise HTTPException(400, "Website name is required.")
    if db.get_website_by_name(name):
        raise HTTPException(409, f"'{name}' is already added.")
    try:
        resolved = await resolve_website(name)
    except RuntimeError as exc:
        raise HTTPException(502, str(exc))
    site = db.add_website(
        name=name, base_url=resolved["base_url"],
        search_url=resolved["search_url"],
        encoding=resolved.get("encoding", "plus"),
    )
    return {"data": _shape_site(site)}


@scout_router.put("/websites")
def update_website(payload: UpdateWebsiteRequest):
    if not db.get_website_by_name(payload.name):
        raise HTTPException(404, f"'{payload.name}' not found.")
    updated = db.update_website(
        name=payload.name, active=payload.active,
        search_url=payload.search_url, base_url=payload.base_url,
    )
    return {"data": _shape_site(updated)}


@scout_router.delete("/websites/{name}")
def delete_website(name: str):
    if not db.get_website_by_name(name):
        raise HTTPException(404, f"'{name}' not found.")
    updated = db.update_website(name=name, active=False)
    return {"status": "deactivated", "data": _shape_site(updated)}


@scout_router.post("/websites/{name}/reactivate")
def reactivate_website(name: str):
    if not db.get_website_by_name(name):
        raise HTTPException(404, f"'{name}' not found.")
    updated = db.update_website(name=name, active=True)
    return {"data": _shape_site(updated)}


@scout_router.post("/search/products")
async def search_single(payload: SearchRequest):
    name      = payload.name.strip()
    platforms = payload.platforms or []
    if not name:
        raise HTTPException(400, "Product name is required.")
    if not platforms:
        platforms = [s["name"] for s in db.get_active_websites()]
    row = await _search_across_sites(name, platforms, force_refresh=payload.force_refresh)
    return {"status": "success", "products": [row]}


@scout_router.get("/products")
def get_all_products():
    rows, platforms = db.get_all_products()
    return {"data": rows, "platforms": platforms}


@scout_router.post("/upload/file")
async def upload_file(
    file:      UploadFile = File(...),
    platforms: str        = Form(None),
):
    content  = await file.read()
    filename = (file.filename or "").lower()
    raw_names: list[str] = []

    if filename.endswith(".csv"):
        try:    text = content.decode("utf-8-sig")
        except: text = content.decode("latin-1")
        reader = csv.reader(io.StringIO(text))
        for i, row in enumerate(reader):
            for cell in row:
                cell = cell.strip()
                if i == 0 and cell.lower() in {"product", "product name", "name", ""}:
                    continue
                if cell:
                    raw_names.append(cell)
    elif filename.endswith((".xlsx", ".xls")):
        try:    df = pd.read_excel(io.BytesIO(content))
        except Exception as exc:
            raise HTTPException(400, f"Invalid Excel file: {exc}")
        for col in df.columns:
            raw_names.extend(df[col].dropna().astype(str).tolist())
    else:
        raise HTTPException(400, "Unsupported file type. Please upload .csv, .xlsx, or .xls")

    product_names = _parse_product_names(raw_names)
    if not product_names:
        raise HTTPException(400, "No product names found in the uploaded file.")

    platforms_list = (
        [p.strip() for p in platforms.split(",") if p.strip()]
        if platforms
        else [s["name"] for s in db.get_active_websites()]
    )
    logger.info(f"[upload] {len(product_names)} products × {len(platforms_list)} platforms")
    return await _run_bulk_search(product_names, platforms_list)


@scout_router.get("/price-history/{product_name}")
def get_price_history(product_name: str, platform: Optional[str] = None, limit: int = 90):
    history = db.get_price_history(product_name, platform, limit)
    grouped: dict[str, list] = {}
    for row in history:
        p = row["platform"]
        grouped.setdefault(p, []).append({
            "price": float(row["price"]), "currency": row["currency"],
            "scraped_at": row["scraped_at"].isoformat(), "url": row.get("url", ""),
        })
    return {"product_name": product_name, "platforms": grouped, "total_points": len(history)}


@scout_router.get("/alerts")
def get_alerts(unacknowledged_only: bool = False, limit: int = 50):
    alerts       = db.get_alerts(unacknowledged_only, limit)
    unread_count = db.get_unacknowledged_count()
    return {
        "unread_count": unread_count,
        "alerts": [
            {
                "id": a["id"], "product_name": a["product_name"],
                "platform": a["platform"],
                "old_price":      float(a["old_price"])      if a["old_price"]      is not None else None,
                "new_price":      float(a["new_price"]),
                "change_amount":  float(a["change_amount"])  if a["change_amount"]  is not None else None,
                "change_percent": float(a["change_percent"]) if a["change_percent"] is not None else None,
                "direction": a["direction"], "url": a.get("url", ""),
                "detected_at": a["detected_at"].isoformat(),
                "acknowledged": a["acknowledged"],
            }
            for a in alerts
        ],
    }


@scout_router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: int):
    if not db.acknowledge_alert(alert_id):
        raise HTTPException(404, f"Alert {alert_id} not found.")
    return {"status": "acknowledged", "alert_id": alert_id}


@scout_router.post("/price-monitor/run")
async def run_price_monitor():
    rows, _ = db.get_all_products()
    if not rows:
        return {"status": "nothing_to_monitor", "products_checked": 0}
    platforms = [s["name"] for s in db.get_active_websites()]
    checked = 0
    for row in rows:
        product_name = row.get("name", "")
        if not product_name: continue
        await _search_across_sites(product_name, platforms, force_refresh=True)
        checked += 1
    alerts = db.get_alerts(limit=checked * 10)
    return {"status": "completed", "products_checked": checked,
            "alerts_generated": len(alerts), "alerts": alerts}


@scout_router.get("/entities/{query}")
def get_entities(query: str):
    entities = get_entities_for_query(db, query)
    return {"query": query, "entities": entities, "count": len(entities)}


@scout_router.get("/compare/{query}")
def compare_prices(query: str):
    entities   = get_entities_for_query(db, query)
    comparable = [e for e in entities if e["platform_count"] >= 2]
    single     = [e for e in entities if e["platform_count"] == 1]
    return {
        "query": query,
        "comparable": [
            {
                "entity_id": e["entity_id"], "product": e["canonical_name"],
                "brand": e["canonical_brand"], "variant": e["canonical_variant"],
                "cheapest": e["best_price"], "price_spread": e["price_spread"],
                "platforms": [
                    {"platform": l["platform"], "price": l["price"]["value"], "url": l["url"]}
                    for l in sorted(e["listings"], key=lambda x: x["price"]["value"])
                ],
            }
            for e in comparable
        ],
        "single_platform": [
            {"entity_id": e["entity_id"], "product": e["canonical_name"],
             "platform": e["listings"][0]["platform"] if e["listings"] else "",
             "price": e["best_price"]}
            for e in single
        ],
        "summary": {
            "total_entities": len(entities), "cross_platform": len(comparable),
            "single_platform": len(single),
            "best_savings": max(
                (e["price_spread"]["diff_percent"] for e in comparable if e.get("price_spread")),
                default=0,
            ),
        },
    }


@scout_router.get("/features/{query}")
def get_feature_comparison(query: str):
    entities = get_entities_for_query(db, query)
    if not entities:
        raise HTTPException(404, f"No results found for '{query}'. Run a search first.")
    entity   = entities[0]
    listings = entity.get("listings", [])
    if not listings:
        raise HTTPException(404, f"No listings found for '{query}'.")

    enriched: list[dict] = []
    for listing in listings:
        platform = listing["platform"]
        cached_feats = db.get_cached_features(query, platform)
        if cached_feats:
            listing["_cached_features"] = cached_feats
            listing["product_details"]  = {}
            enriched.append(listing)
            continue
        with db._conn() as conn:
            row = db._fetchone(conn, """
                SELECT product_details, product_url, scraped_at
                FROM product_results
                WHERE product_name = %s AND platform = %s
                ORDER BY scraped_at DESC LIMIT 1
            """, (query, platform))
        if row:
            details = row.get("product_details") or {}
            if isinstance(details, str):
                try:    details = json.loads(details)
                except: details = {}
            listing["product_details"] = details
            listing["last_seen"] = row["scraped_at"].isoformat() if row.get("scraped_at") else None
        enriched.append(listing)

    result = compare_features(query, enriched)
    for pd_ in result.get("platform_details", []):
        platform = pd_["platform"]
        if not db.get_cached_features(query, platform):
            db.save_features(
                product_name=query, platform=platform,
                category=result["category"],
                product_feats=pd_.get("product_features", {}),
                platform_feats=pd_.get("platform_features", {}),
            )
    return result