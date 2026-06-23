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
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from .scout_db import db
from app.auth_router import get_current_user
from .entity_resolver import (
    get_entities_for_query,
    resolve_entities,
    save_entities,
    extract_brand_from_title,
)
from .feature_extractor import compare_features
from .resolver import resolve_website
from .scraper import scrape_product_on_site
from .cancellation import (
    register_search,
    cancel_search,
    cleanup_search,
    check_cancelled,
    SearchCancelledException,
)
from . import langfuse_config                       # stays at top-level, NOT in scout/

logger = logging.getLogger(__name__)

scout_router = APIRouter(tags=["scout"])


def _client_id(user: dict) -> str:
    """Extract the single client_id from the authenticated user's clientAccess list.
    super_admin has clientAccess=["*"] — for scout we default to "" (all platforms)
    so admins still see everything. Regular client users get their own tenant scope."""
    access = user.get("clientAccess") or []
    if "*" in access or not access:
        return ""
    return access[0]

CACHE_TTL_MINUTES = 120
BULK_CONCURRENCY  = 5  


# ── Pydantic models ───────────────────────────────────────────────────

class AddWebsiteRequest(BaseModel):
    name:       str
    icon:       str = "🌐"            # emoji shown next to platform name in UI
    request_id: Optional[str] = None  # for cooperative cancel — frontend sends UUID

class UpdateWebsiteRequest(BaseModel):
    name:       str
    search_url: str
    base_url:   Optional[str] = None
    active:     bool = True
    icon:       Optional[str] = None  # if omitted, existing icon is preserved

class SearchRequest(BaseModel):
    name:          str
    platforms:     Optional[list[str]] = []
    force_refresh: bool = False
    request_id:    Optional[str] = None  # for cooperative cancel — frontend sends UUID


# ── Helpers ───────────────────────────────────────────────────────────

def _shape_site(site: dict) -> dict:
    return {
        "name":       site["name"],
        "search_url": site.get("search_url", ""),
        "base_url":   site.get("base_url", ""),
        "encoding":   site.get("encoding", "plus"),
        "active":     bool(site["active"]),
        "icon":       site.get("icon", "🌐"),
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
    request_id:     Optional[str] = None,
    client_id: str = "",
) -> dict:
    sites   = db.get_active_websites(client_id)
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
                result = await scrape_product_on_site(site, variant, request_id=request_id)
                if result and result.get("listings"):
                    best = result["listings"][0]
                    logger.info(f"[search] ✅ {platform_name} worked with: '{variant}'")
                    return best
            except SearchCancelledException:
                # User cancelled — stop trying further variants and propagate up
                # so asyncio.gather sees this platform as cancelled.
                logger.info(f"[search] 🛑 {platform_name} cancelled by user")
                raise
            except Exception as e:
                logger.error(f"[search] Error on {platform_name} with '{variant}': {e}")
        logger.warning(f"[search] 🚫 {platform_name} failed for ALL variants")
        return None

    results = await asyncio.gather(
        *[_scrape_one_platform(site) for site in platforms_to_scrape],
        return_exceptions=True,
    )

    # If any platform was cancelled by the user, propagate the cancel up to
    # the route handler so it can return a 499 / cancelled status. We do this
    # BEFORE saving partial results — when the user cancels mid-bulk-search,
    # we don't want to half-commit results to the DB.
    for res in results:
        if isinstance(res, SearchCancelledException):
            raise res

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
                client_id=client_id,
            )

    return final_product


async def _run_bulk_search(
    product_names: list[str],
    platforms:     list[str],
    request_id:    Optional[str] = None,
) -> dict:
    semaphore = asyncio.Semaphore(BULK_CONCURRENCY)

    async def _limited(name: str) -> dict:
        async with semaphore:
            try:
                return await _search_across_sites(name, platforms, request_id=request_id)
            except SearchCancelledException:
                # Propagate up through gather so the caller can return cancelled
                raise
            except Exception as exc:
                logger.error(f"Bulk search error for '{name}': {exc}")
                return {"name": name, "listings": []}

    results = await asyncio.gather(
        *[_limited(name) for name in product_names],
        return_exceptions=True,
    )

    # If user cancelled, surface that up. The cancel may have arrived
    # between products in the bulk batch — partial results are discarded
    # to keep the user's mental model simple ("cancel = nothing happened").
    for res in results:
        if isinstance(res, SearchCancelledException):
            raise res

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
def get_platforms(user: dict = Depends(get_current_user)):
    cid = _client_id(user)
    sites = db.get_active_websites(cid)
    return {"platforms": [s["name"] for s in sites]}


@scout_router.get("/websites/all")
def get_all_websites(user: dict = Depends(get_current_user)):
    cid = _client_id(user)
    sites = db.get_all_websites(cid)
    return {"data": [_shape_site(s) for s in sites]}


def _canonical_platform_name(raw: str) -> str:
    import re as _re
    s = raw.strip().lower()
    s = _re.sub(r'^https?://', '', s)
    s = _re.sub(r'^www\.', '', s)
    s = s.split('/')[0].split('?')[0].split('#')[0]
    # Only strip TLD if input has NO dot (typed as brand name like "amazon")
    # If input has a dot (like "amazon.in"), keep the full domain as-is
    # so amazon.com and amazon.in are treated as different platforms
    if '.' not in raw.strip():
        s = _re.sub(
            r'\.(com\.au|co\.uk|co\.in|com|net|org|in|io|co|store|shop|app)$',
            '', s,
        )
    return s.strip()


@scout_router.post("/websites")
async def add_website(payload: AddWebsiteRequest, user: dict = Depends(get_current_user)):
    name = payload.name.strip()
    if not name:
        raise HTTPException(400, "Website name is required.")

    # Normalise before the duplicate check so 'Flipkart', 'flipkart',
    # 'flipkart.com' and 'www.flipkart.com' all resolve to the same key.
    canonical = _canonical_platform_name(name)
    if not canonical:
        raise HTTPException(400, "Could not derive a platform name from the input.")

    # Case-insensitive, TLD-insensitive duplicate check across all sites.
    cid = _client_id(user)
    existing_sites = db.get_all_websites(cid)
    name_has_dot = '.' in name.strip()
    for site in existing_sites:
        existing_canonical = _canonical_platform_name(site["name"])
        existing_has_dot = '.' in site["name"].strip()
        if existing_canonical == canonical:
        # If new input has a dot → let resolver run first,
        # then compare actual resolved domains post-resolution.
        # This allows amazon.com when amazon.in already exists.
            if name_has_dot:
                continue
            raise HTTPException(
                409,
                f"'{site['name']}' is already added. "
                f"('{name}' resolves to the same platform.)"
            )

    # Register this add-website job in the cancellation registry BEFORE the
    # slow work starts. If we registered after, the user's Cancel click
    # would arrive before the registry entry exists and would be silently
    # dropped. Frontend always sends request_id; internal callers may not.
    request_id = payload.request_id
    if request_id:
        register_search(request_id)

    try:
        try:
            resolved = await resolve_website(name, request_id=request_id)
        except RuntimeError as exc:
            msg = str(exc)
            if msg.startswith("BOT_PROTECTED:"):
                base = msg.split(":", 1)[1]
                raise HTTPException(422,
                    f"'{name}' ({base}) uses anti-bot protection that blocked our detection. "                    f"You can still add it manually with the correct search URL.")
            elif msg.startswith("URL_UNKNOWN:"):
                base = msg.split(":", 1)[1]
                raise HTTPException(422,
                    f"'{name}' ({base}) uses anti-bot protection that blocked our detection. "                    f"You can still add it manually with the correct search URL.")
            else:
                raise HTTPException(422,
                    f"'{name}' uses anti-bot protection that blocked our detection. "                    f"You can still add it manually with the correct search URL.")

        # Final cancel check between resolver completion and the DB write.
        # The resolver's last phase (LLM URL guess) is non-cancellable once
        if name_has_dot:
            import urllib.parse as _urlparse
            new_domain = _urlparse.urlparse(resolved["base_url"]).netloc.lower()
            for site in existing_sites:
                if '.' in site["name"].strip():
                    existing_domain = _urlparse.urlparse(
                        site.get("search_url", "")
                    ).netloc.lower()
                    if existing_domain and existing_domain == new_domain:
                        raise HTTPException(
                            409,
                            f"'{site['name']}' is already added. "
                            f"('{name}' resolves to the same platform.)"
                        )

        check_cancelled(request_id)

        site = db.add_website(
            name=name, base_url=resolved["base_url"],
            search_url=resolved["search_url"],
            encoding=resolved.get("encoding", "plus"),
            client_id=cid,
            icon=payload.icon,
        )
        
        return {"data": _shape_site(site)}
    except SearchCancelledException:
        # User clicked Cancel. Return the same shape as /search/products
        # cancel responses so the frontend's handler can be uniform.
        logger.info(f"[add_website] Cancelled by user: request_id={request_id}")
        return {"status": "cancelled", "data": None, "request_id": request_id}
    finally:
        # Always clean up the registry entry — on success, on cancel, or
        # on any unhandled error — otherwise we leak Event objects.
        cleanup_search(request_id)


@scout_router.put("/websites")
def update_website(payload: UpdateWebsiteRequest, user: dict = Depends(get_current_user)):
    cid = _client_id(user)
    if not db.get_website_by_name(payload.name, cid):
        raise HTTPException(404, f"'{payload.name}' not found.")
    updated = db.update_website(
        name=payload.name, active=payload.active,
        search_url=payload.search_url, base_url=payload.base_url,
        client_id=cid, icon=payload.icon,
    )
    return {"data": _shape_site(updated)}


@scout_router.delete("/websites/{name}")
def delete_website(name: str, user: dict = Depends(get_current_user)):
    """
    Permanently delete a website and all its associated data.
    This is NOT reversible. Use PUT /websites with active=false for soft-delete.
    """
    cid = _client_id(user)
    try:
        counts = db.delete_website(name, cid)
    except ValueError:
        raise HTTPException(404, f"'{name}' not found.")
    return {
        "status": "deleted",
        "name": name,
        "deleted_counts": counts,
    }


@scout_router.post("/websites/{name}/reactivate")
def reactivate_website(name: str, user: dict = Depends(get_current_user)):
    cid = _client_id(user)
    if not db.get_website_by_name(name, cid):
        raise HTTPException(404, f"'{name}' not found.")
    updated = db.update_website(name=name, active=True, client_id=cid)
    return {"data": _shape_site(updated)}


@scout_router.post("/websites/cancel/{request_id}")
def cancel_add_website(request_id: str):
    """
    Cancel an in-flight add-website operation. Frontend POSTs here with the
    same request_id it sent in the original /websites call.

    Parallel to /search/cancel/{request_id} — same registry, different URL.
    Kept separate because the registry entry is owned by the route that
    created it; conflating endpoints would make later divergence (different
    timeouts, different audit logging) harder to manage.

    Returns 200 even if the request_id is unknown (already finished or
    invalid) — user-facing behavior is the same: nothing is running. The
    `cancelled: false` flag in the response is for debugging only.
    """
    cancelled = cancel_search(request_id)
    if cancelled:
        logger.info(f"[add_website] Cancel signal sent for request_id={request_id}")
    else:
        logger.info(f"[add_website] Cancel requested for unknown/finished request_id={request_id}")
    return {"status": "ok", "cancelled": cancelled, "request_id": request_id}


@scout_router.post("/search/products")
async def search_single(payload: SearchRequest, user: dict = Depends(get_current_user)):
    cid       = _client_id(user)
    name      = payload.name.strip()
    platforms = payload.platforms or []
    if not name:
        raise HTTPException(400, "Product name is required.")
    if not platforms:
        platforms = [s["name"] for s in db.get_active_websites(cid)]

    # Register this search in the cancellation registry. If the client passed
    # a request_id (UUID generated by the frontend), we use it; otherwise we
    # skip cancellation support for this call. The frontend always sends one
    # for user-facing searches; internal scripts may not.
    request_id = payload.request_id
    if request_id:
        register_search(request_id)

    try:
        row = await _search_across_sites(
            name,
            platforms,
            force_refresh=payload.force_refresh,
            request_id=request_id,
            client_id=cid,
        )
        return {"status": "success", "products": [row]}
    except SearchCancelledException:
        # User clicked Cancel. Return a clear cancelled status — the frontend
        # treats this as "stopped, do nothing" rather than as an error.
        logger.info(f"[search] Cancelled by user: request_id={request_id}")
        return {"status": "cancelled", "products": [], "request_id": request_id}
    finally:
        # Always clean up the registry entry, even on success or unhandled
        # error — otherwise we leak Event objects forever.
        cleanup_search(request_id)


@scout_router.post("/search/cancel/{request_id}")
def cancel_search_endpoint(request_id: str):
    """
    Cancel an in-flight search. Frontend POSTs here with the same request_id
    it sent in the original /search/products call.
    Returns 200 even if the search wasn't found (already finished or invalid
    ID) — the user-facing behavior is the same: the search is no longer
    running. We just include `cancelled: false` so debugging is possible.
    """
    cancelled = cancel_search(request_id)
    if cancelled:
        logger.info(f"[search] Cancel signal sent for request_id={request_id}")
    else:
        logger.info(f"[search] Cancel requested for unknown/finished request_id={request_id}")
    return {"status": "ok", "cancelled": cancelled, "request_id": request_id}


@scout_router.get("/products")
def get_all_products(limit: int = 0, offset: int = 0, user: dict = Depends(get_current_user)):
    """
    List tracked products.

    - limit=0 (default): return ALL products (backward compatible)
    - limit>0: paginate; also returns `total` so UI can render
      "Showing X-Y of N" and manage prev/next state.
    """
    cid = _client_id(user)
    rows, platforms, total = db.get_all_products(limit=limit, offset=offset, client_id=cid)
    return {"data": rows, "platforms": platforms, "total": total}


@scout_router.post("/upload/file")
async def upload_file(
    file:       UploadFile     = File(...),
    platforms:  str            = Form(None),
    request_id: Optional[str]  = Form(None),
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
        else [s["name"] for s in db.get_active_websites(_client_id(user))]
    )
    logger.info(f"[upload] {len(product_names)} products × {len(platforms_list)} platforms")

    # Register the bulk search for cancellation, mirroring /search/products.
    # If request_id is None (unusual — the frontend always sends one), we
    # skip the registry and the run is just non-cancellable.
    if request_id:
        register_search(request_id)

    try:
        return await _run_bulk_search(product_names, platforms_list, request_id=request_id)
    except SearchCancelledException:
        logger.info(f"[upload] Cancelled by user: request_id={request_id}")
        return {"status": "cancelled", "products": [], "request_id": request_id}
    finally:
        cleanup_search(request_id)


@scout_router.get("/price-history/{product_name}")
def get_price_history(product_name: str, platform: Optional[str] = None, limit: int = 90, user: dict = Depends(get_current_user)):
    cid = _client_id(user)
    history = db.get_price_history(product_name, platform, limit, client_id=cid)
    grouped: dict[str, list] = {}
    for row in history:
        p = row["platform"]
        grouped.setdefault(p, []).append({
            "price": float(row["price"]), "currency": row["currency"],
            "scraped_at": row["scraped_at"].isoformat(), "url": row.get("url", ""),
        })
    return {"product_name": product_name, "platforms": grouped, "total_points": len(history)}


@scout_router.get("/alerts")
def get_alerts(
    unacknowledged_only: bool = False,
    limit:               int  = 50,
    offset:              int  = 0,
    user: dict = Depends(get_current_user),
    ):
    """
    List price alerts, newest first.
    Returns `total` so the UI can paginate: "Showing X-Y of N".
    `unread_count` is separate — it always counts across the full table,
    not just the current page.
    """
    cid = _client_id(user)
    alerts, total = db.get_alerts(unacknowledged_only, limit, offset, client_id=cid)
    unread_count  = db.get_unacknowledged_count(cid)
    return {
        "unread_count": unread_count,
        "total":        total,
        "alerts": [
            {
                "id": a["id"], "product_name": a["product_name"],
                "title": a.get("title", a["product_name"]),
                "platform": a["platform"],
                "currency": a.get("currency", ""),
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
async def run_price_monitor(user: dict = Depends(get_current_user)):
    cid = _client_id(user)
    # get_all_products() now returns (rows, platforms, total). We only need rows here.
    rows, _, _ = db.get_all_products(client_id=cid)
    if not rows:
        return {"status": "nothing_to_monitor", "products_checked": 0}
    platforms = [s["name"] for s in db.get_active_websites(cid)]
    checked = 0
    for row in rows:
        product_name = row.get("name", "")
        if not product_name: continue
        await _search_across_sites(product_name, platforms, force_refresh=True, client_id=cid)
        checked += 1
    # get_alerts() now returns (alerts, total). We only need alerts here.
    alerts, _ = db.get_alerts(limit=checked * 10, client_id=cid)
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
                    {
                        "platform": l["platform"],
                        "price":    l["price"]["value"],
                        "currency": l["price"]["currency"],
                        "url":      l["url"],
                    }
                    for l in sorted(e["listings"], key=lambda x: x["price"]["value"])
                ],
            }
            for e in comparable
        ],
        "single_platform": [
            {"entity_id": e["entity_id"], "product": e["canonical_name"],
             "platform": e["listings"][0]["platform"] if e["listings"] else "",
             "price":    e["best_price"],
             "currency": e["listings"][0]["price"]["currency"] if e["listings"] else "INR"}
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