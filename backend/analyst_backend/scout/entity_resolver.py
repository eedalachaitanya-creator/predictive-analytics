"""
scout/entity_resolver.py — Universal entity resolution across platforms

Works for ANY product category:
  - Electronics (phones, laptops, appliances)
  - Health & supplements
  - Fashion (clothing, watches, accessories)
  - Food & grocery
  - Books, toys, sports gear — anything

3-stage pipeline:
  Stage 1: Structural pre-filters (brand mismatch → reject immediately)
  Stage 2: Fuzzy similarity scoring (token overlap + sequence match)
  Stage 3: LLM arbitration (only for ambiguous scores 0.45–0.80)
"""

import os, re, json, uuid, logging
from difflib import SequenceMatcher
from openai import OpenAI

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Use Langfuse-instrumented OpenAI client for automatic cost tracking.
# LAZY init — don't create at import time because load_dotenv() in main.py
# may not have run yet. The getter ensures the client exists when first needed.
_openai_client = None

def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from .langfuse_config import get_openai_client
        _openai_client = get_openai_client()
    return _openai_client

# ── Thresholds ────────────────────────────────────────────────────────
SAME_THRESHOLD      = 0.82   # above → same, skip LLM
DIFFERENT_THRESHOLD = 0.38   # below → different, skip LLM
# between → ask LLM

# ── Universal Noise Words ─────────────────────────────────────────────
# Words that appear in almost every product title but carry no identity signal

NOISE_WORDS = {
    # Articles / prepositions
    'a', 'an', 'the', 'of', 'in', 'at', 'by', 'to', 'is', 'and', 'or',
    'for', 'with', 'from', 'that', 'this', 'on', 'as',

    # Commerce noise
    # Commerce noise
    'buy', 'online', 'price', 'offer', 'deal', 'sale', 'discount',
    'best', 'new', 'top', 'free', 'delivery', 'shipping',
    'combo', 'pack', 'set', 'kit', 'bundle',
    'original', 'genuine', 'authentic', 'certified', 'official',
    'imported', 'edition',
    # NOTE: 'india' intentionally removed — it's part of "Organic India" brand name

    # Quality descriptors that don't identify a product
    'premium', 'professional', 'pro', 'ultra', 'super', 'mega', 'mini',
    'advanced', 'plus', 'max', 'lite', 'smart',
}


# ── Universal Variant/Spec Extraction ────────────────────────────────

# Patterns that identify a specific variant of a product
# Ordered from most specific to least specific

VARIANT_PATTERNS = [
    # Storage / memory (electronics)
    (r'(\d+)\s*(tb|gb|mb)', 'storage'),
    # RAM
    (r'(\d+)\s*gb\s*ram', 'ram'),
    # Screen size
    (r'(\d+(?:\.\d+)?)\s*(?:inch|")', 'screen'),
    # Resolution
    (r'(4k|2k|1080p|720p|hd|fhd|qhd|uhd)', 'resolution'),
    # Weight (grams, kg)
    (r'(\d+(?:\.\d+)?)\s*(kg|g)\b(?!\s*(?:capsule|tablet|softgel))', 'weight'),
    # Volume (ml, l, fl oz)
    (r'(\d+(?:\.\d+)?)\s*(ml|l|litre|liter|fl\s*oz)', 'volume'),
    # Count of items (generic)
    (r'(\d+)\s*(pcs|pieces|units|count|ct)\b', 'count'),
    # Clothing size
    (r'\b(xs|s|m|l|xl|xxl|xxxl|2xl|3xl|size\s+\d+)\b', 'size'),
    # Shoe size
    (r'\b(uk\s*\d+|us\s*\d+|eu\s*\d+)\b', 'shoe_size'),
    # Pharmaceutical count (kept but not prioritized)
    (r'(\d+)\s*(capsules?|tablets?|softgels?|gummies|sachets?)', 'pill_count'),
    # Pharmaceutical dosage
    (r'(\d+(?:\.\d+)?)\s*(mg|mcg|iu)\b', 'dosage'),
    # Color — only as supplementary signal, not primary
    (r'\b(black|white|silver|gold|blue|red|green|pink|grey|gray|'
     r'rose\s*gold|space\s*gray|midnight|starlight|coral|navy|beige|brown)\b', 'color'),
    # Wattage / power
    (r'(\d+)\s*w\b(?!atch)', 'wattage'),
    # RPM / speed
    (r'(\d+)\s*rpm\b', 'rpm'),
    # Generic number + unit fallback
    (r'(\d+(?:\.\d+)?)\s*([a-z]{1,4})\b', 'generic_unit'),
]


def extract_all_variants(title: str) -> dict:
    """
    Extract all variant/spec signals from a product title.
    Returns dict of {variant_type: normalized_value}.
    Works for any product category.
    """
    title_lower = title.lower()
    variants = {}

    for pattern, variant_type in VARIANT_PATTERNS:
        match = re.search(pattern, title_lower, re.IGNORECASE)
        if match:
            # Normalize value
            value = match.group(0).lower()
            value = re.sub(r'\s+', '', value)  # "32 gb" → "32gb"
            variants[variant_type] = value

    return variants


def variants_conflict(variants_a: dict, variants_b: dict) -> bool:
    """
    Check if two variant sets conflict — meaning they are DIFFERENT variants
    of the same product (e.g. 64GB vs 128GB, XL vs M).
    
    Only conflicts if BOTH have the same variant type but different values.
    Missing variant = no conflict (might just be missing from title).
    Color conflicts are soft — don't hard-block.
    """
    HARD_CONFLICT_TYPES = {
        'storage', 'ram', 'screen', 'volume',
        'pill_count', 'dosage', 'shoe_size',
        'wattage', 'count', 'weight',   # ← added weight
    }

    SOFT_CONFLICT_TYPES = {'color', 'size', 'resolution'}

    for vtype in HARD_CONFLICT_TYPES:
        if vtype in variants_a and vtype in variants_b:
            if variants_a[vtype] != variants_b[vtype]:
                logger.debug(f"Hard variant conflict on {vtype}: {variants_a[vtype]} vs {variants_b[vtype]}")
                return True

    # Soft conflicts: size or color mismatch — flag but don't hard-block
    # (handled as penalty in similarity score instead)
    return False


# ── Universal Brand Extraction ────────────────────────────────────────

# Words that signal the brand name has ended and product description begins
BRAND_STOP_WORDS = {
    # Product type nouns
    'capsule', 'capsules', 'tablet', 'tablets', 'powder', 'liquid',
    'phone', 'laptop', 'watch', 'shoe', 'shirt', 'cream', 'serum',
    'vitamin', 'supplement', 'protein', 'oil', 'juice', 'tea',
    'charger', 'cable', 'case', 'cover', 'bag', 'bottle', 'dryer',
    'headphone', 'earphone', 'speaker', 'camera', 'monitor', 'tv',
    'hair', 'face', 'body', 'skin', 'hand', 'foot', 'eye', 'lip',
    'shampoo', 'conditioner', 'lotion', 'sunscreen', 'moisturizer',

    # Descriptors that are NOT brand words
    'pure', 'original', 'natural', 'herbal', 'ayurvedic',
    'professional', 'advanced', 'ultra', 'super', 'mega', 'mini',
    'vintage', 'classic', 'modern', 'new', 'improved', 'premium',
    'supersonic', 'detect', 'cyclone', 'turbo', 'boost', 'charge',
    'galaxy', 'iphone', 'ipad', 'macbook', 'pixel', 'xperia',

    # Herb/ingredient names — common in health products, never a brand
    'ashwagandha', 'arjuna', 'turmeric', 'ginger', 'tulsi', 'neem',
    'brahmi', 'triphala', 'giloy', 'amla', 'moringa', 'spirulina',
    'wheatgrass', 'aloe', 'vera', 'garcinia', 'green', 'tea',
    'omega', 'collagen', 'biotin', 'melatonin', 'glutathione',

    # Product descriptor words common in health titles
    'iodized', 'fortified', 'enriched', 'flavored', 'unflavored',
    'sweetened', 'unsweetened', 'decaf', 'instant',

    # Product attributes
    'black', 'white', 'silver', 'gold', 'blue', 'red', 'green',
    'pink', 'grey', 'gray', 'titanium', 'midnight', 'starlight',
    'small', 'medium', 'large', 'xl', 'xxl',
}


TWO_WORD_BRANDS = {
    'organic india', 'tata salt', 'tata tea', 'tata motors',
    'fast&up', 'fastup', 'now foods', 'garden of life',
    'nature made', 'wellbeing nutrition', 'wow skin',
}

def extract_brand_from_title(title: str) -> str | None:
    """
    Extract brand from product title WITHOUT a hardcoded brand list.
    Brand = first 1 word (occasionally 2) before any product-type or
    descriptor word. Stops as soon as it hits a non-brand word.
    """
    if not title:
        return None

    title_lower = title.lower()
    for brand in TWO_WORD_BRANDS:
        if title_lower.startswith(brand) or f' {brand}' in title_lower[:30]:
            return brand
        
    # Work with original casing for proper-noun detection
    words = title.strip().split()
    if not words:
        return None

    brand_words = []

    for i, word in enumerate(words):
        clean = re.sub(r'[^\w]', '', word).lower()

        # Stop conditions
        if not clean or len(clean) < 2:
            break
        if clean in NOISE_WORDS:
            continue
        if clean in BRAND_STOP_WORDS:
            break
        # Stop if it looks like a model number (starts with digit or is alphanumeric code)
        if re.match(r'^\d', clean):
            break
        if re.match(r'^[a-z]{1,3}\d+', clean):  # e.g. "hd15", "v15", "s24"
            break
        # Stop after 2 brand words max — brands are rarely 3+ words
        if len(brand_words) >= 2:
            break

        brand_words.append(clean)

    if not brand_words:
        return None

    return ' '.join(brand_words)


def brands_conflict(brand_a: str | None, brand_b: str | None) -> bool:
    """
    Check if two brands conflict. Handles abbreviations and partial matches.
    'samsung' vs 'samsung electronics' → no conflict
    'apple' vs 'samsung' → conflict
    None vs anything → no conflict (brand not detected)
    """
    if not brand_a or not brand_b:
        return False  # Can't determine conflict without both brands

    a = brand_a.lower().strip()
    b = brand_b.lower().strip()

    if a == b:
        return False

    # One is a prefix/substring of the other (e.g. "samsung" in "samsung electronics")
    if a in b or b in a:
        return False

    # Token overlap — "organic india" vs "organic india pvt" → overlap
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if tokens_a & tokens_b:
        return False

    return True


# ── Normalization ─────────────────────────────────────────────────────

def normalize_title(title: str) -> str:
    """
    Normalize a product title for similarity comparison.
    Category-agnostic — works for any product type.
    """
    t = title.lower().strip()

    # Remove special characters
    t = re.sub(r'[^\w\s]', ' ', t)

    # Collapse whitespace
    t = re.sub(r'\s+', ' ', t)

    # Remove noise words
    words = [w for w in t.split() if w not in NOISE_WORDS and len(w) > 1]

    return ' '.join(words)


# ── Similarity Scoring ────────────────────────────────────────────────

def token_overlap_score(a: str, b: str) -> float:
    """Jaccard similarity on normalized token sets."""
    tokens_a = set(normalize_title(a).split())
    tokens_b = set(normalize_title(b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union        = tokens_a | tokens_b
    return len(intersection) / len(union)


def sequence_similarity(a: str, b: str) -> float:
    """SequenceMatcher ratio on normalized titles."""
    return SequenceMatcher(
        None,
        normalize_title(a),
        normalize_title(b)
    ).ratio()


def compute_similarity(title_a: str, title_b: str) -> dict:
    """
    Compute similarity score between two product titles.
    Category-agnostic — applies structural penalties for any product type.
    
    Returns dict with score (0-1) and full breakdown for debugging.
    """
    token_score    = token_overlap_score(title_a, title_b)
    sequence_score = sequence_similarity(title_a, title_b)

    # Weighted combination
    combined = (token_score * 0.65) + (sequence_score * 0.35)

    penalties = []

    # Variant conflict penalty
    variants_a = extract_all_variants(title_a)
    variants_b = extract_all_variants(title_b)

    if variants_conflict(variants_a, variants_b):
        combined = max(0.0, combined - 0.30)
        penalties.append(f"variant_conflict (-0.30)")

    # Brand conflict penalty
    brand_a = extract_brand_from_title(title_a)
    brand_b = extract_brand_from_title(title_b)

    if brands_conflict(brand_a, brand_b):
        combined = max(0.0, combined - 0.40)
        penalties.append(f"brand_conflict {brand_a} vs {brand_b} (-0.40)")

    return {
        "score":          round(combined, 4),
        "token_score":    round(token_score, 4),
        "sequence_score": round(sequence_score, 4),
        "variants_a":     variants_a,
        "variants_b":     variants_b,
        "brand_a":        brand_a,
        "brand_b":        brand_b,
        "penalties":      penalties,
        "norm_a":         normalize_title(title_a),
        "norm_b":         normalize_title(title_b),
    }


# ── LLM Arbitration ───────────────────────────────────────────────────

def llm_is_same_product(
    title_a: str, platform_a: str,
    title_b: str, platform_b: str,
    query: str
) -> bool:
    """
    LLM disambiguation — called only for ambiguous similarity scores.
    Prompt is fully category-agnostic.
    """
    if not _get_openai_client():
        logger.warning("LLM unavailable for entity resolution — defaulting to False")
        return False

    try:
        response = _get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            # model="gemini-2.5-flash",
            temperature=0,
            max_tokens=10,
            messages=[{
                "role": "user",
                "content": f"""
User searched for: "{query}"

Listing A ({platform_a}): "{title_a}"
Listing B ({platform_b}): "{title_b}"

Are these two listings the EXACT SAME product that a customer would consider interchangeable when comparing prices across platforms?

RULES — answer NO if:
- Different brands (Apple vs Samsung, Organic India vs Himalaya)
- Different models (iPhone 14 vs iPhone 15, Galaxy S23 vs S24)
- Different sizes/variants (64GB vs 128GB, XL vs M, 60 capsules vs 120 capsules)
- Different product types (phone vs phone case, watch vs watch strap)
- Different forms of the same product (tablet vs liquid vs powder vs capsule)

RULES — answer YES if:
- Same product with different title phrasing or word order
- Same product with minor descriptor differences (e.g. "Pure" or "Original" added)
- Same product where one title has more detail than the other

Answer ONLY "yes" or "no".
"""
            }]
        )

        answer = response.choices[0].message.content.strip().lower()
        result = "yes" in answer
        logger.info(
            f"LLM entity resolution [{platform_a}] vs [{platform_b}]: "
            f"{'SAME' if result else 'DIFFERENT'} — "
            f"'{title_a[:40]}' vs '{title_b[:40]}'"
        )
        return result

    except Exception as e:
        logger.warning(f"LLM entity resolution failed: {e}")
        return False


# ── Core Resolution ───────────────────────────────────────────────────

def resolve_entities(listings: list[dict], query: str,
                     llm_fallback: str = "conservative") -> list[dict]:
    """
    llm_fallback: what to do when LLM is unavailable and score is ambiguous
      "conservative" → treat as different (default for production, avoids false grouping)
      "optimistic"   → treat as same (useful for testing without OpenAI key)
      "threshold"    → use midpoint 0.62 as hard cutoff
    """
   
    if not listings:
        return []

    # Only resolve listings that actually found a product
    valid = [l for l in listings if l.get("price", {}).get("value", 0) > 0]

    if not valid:
        return []

    # If only one listing, no resolution needed
    if len(valid) == 1:
        return [_build_entity([valid[0]], query)]

    # Union-Find grouping
    n        = len(valid)
    assigned = [False] * n
    groups   = []

    for i in range(n):
        if assigned[i]:
            continue

        group = [i]
        assigned[i] = True

        title_a    = _get_title(valid[i])
        platform_a = valid[i].get("platform", "")

        for j in range(i + 1, n):
            if assigned[j]:
                continue

            title_b    = _get_title(valid[j])
            platform_b = valid[j].get("platform", "")

            sim = compute_similarity(title_a, title_b)
            score = sim["score"]

            logger.debug(
                f"[{platform_a}] vs [{platform_b}] score={score:.3f} "
                f"penalties={sim['penalties']}"
            )

            if score >= SAME_THRESHOLD:
                group.append(j)
                assigned[j] = True
                logger.info(
                    f"✅ SAME (score={score:.3f}, no LLM): "
                    f"[{platform_a}] '{title_a[:40]}' == [{platform_b}] '{title_b[:40]}'"
                )

            elif score >= DIFFERENT_THRESHOLD:
                if _get_openai_client():
                    is_same = llm_is_same_product(
                        title_a, platform_a,
                        title_b, platform_b,
                        query
                    )
                else:
                    # LLM unavailable — apply fallback strategy
                    if llm_fallback == "optimistic":
                        is_same = True
                        logger.debug(f"LLM fallback=optimistic → treating as SAME")
                    elif llm_fallback == "threshold":
                        is_same = score >= 0.45
                        logger.debug(f"LLM fallback=threshold → score {score:.3f} → {'SAME' if is_same else 'DIFFERENT'}")
                    else:  # conservative
                        is_same = False
                        logger.debug(f"LLM fallback=conservative → treating as DIFFERENT")

                if is_same:
                    group.append(j)
                    assigned[j] = True
                    logger.info(
                        f"✅ LLM SAME (score={score:.3f}): "
                        f"[{platform_a}] '{title_a[:40]}' == [{platform_b}] '{title_b[:40]}'"
                    )
                else:
                    logger.info(
                        f"❌ LLM DIFFERENT (score={score:.3f}): "
                        f"[{platform_a}] '{title_a[:40]}' != [{platform_b}] '{title_b[:40]}'"
                    )
            else:
                logger.info(
                    f"❌ DIFFERENT (score={score:.3f}, no LLM): "
                    f"[{platform_a}] '{title_a[:40]}' != [{platform_b}] '{title_b[:40]}'"
                )

        groups.append([valid[i] for i in group])

    # Build entity objects and sort by relevance to query
    entities = [_build_entity(group, query) for group in groups]

    query_tokens = set(normalize_title(query).split())
    entities.sort(
        key=lambda e: len(query_tokens & set(normalize_title(e["canonical_name"]).split())),
        reverse=True
    )

    return entities


def _get_title(listing: dict) -> str:
    """Extract the best available title from a listing."""
    return (
        listing.get("title") or
        listing.get("name") or
        listing.get("platform", "unknown")
    )


def _build_entity(listings: list[dict], query: str) -> dict:
    """Build a single entity object from a group of matched listings."""
    # Pick canonical listing — prefer longest/most descriptive title
    canonical = max(listings, key=lambda l: len(_get_title(l)))
    canonical_title   = _get_title(canonical)
    canonical_brand   = extract_brand_from_title(canonical_title)
    canonical_variant = _summarize_variants(extract_all_variants(canonical_title))

    # Price analysis
    prices = [
        {"platform": l["platform"], "price": l["price"]["value"]}
        for l in listings
        if l.get("price", {}).get("value", 0) > 0
    ]
    prices.sort(key=lambda p: p["price"])

    best_price  = prices[0]  if prices else None
    worst_price = prices[-1] if prices else None

    price_spread = None
    if best_price and worst_price and best_price["price"] != worst_price["price"]:
        diff_pct = round(
            ((worst_price["price"] - best_price["price"]) / best_price["price"]) * 100,
            2
        )
        price_spread = {
            "min":              best_price["price"],
            "max":              worst_price["price"],
            "diff_percent":     diff_pct,
            "cheapest":         best_price["platform"],
            "most_expensive":   worst_price["platform"],
            "savings":          round(worst_price["price"] - best_price["price"], 2),
        }

    return {
        "entity_id":         str(uuid.uuid4()),
        "canonical_name":    canonical_title,
        "canonical_brand":   canonical_brand,
        "canonical_variant": canonical_variant,
        "query":             query,
        "listings":          listings,
        "best_price":        best_price,
        "price_spread":      price_spread,
        "platform_count":    len(listings),
    }


def _summarize_variants(variants: dict) -> str | None:
    """Convert variant dict to a human-readable string."""
    if not variants:
        return None
    PRIORITY = ['storage', 'ram', 'screen', 'pill_count', 'dosage',
                'volume', 'weight', 'size', 'color', 'resolution']
    parts = []
    for vtype in PRIORITY:
        if vtype in variants:
            parts.append(variants[vtype])
    return ', '.join(parts[:3]) if parts else None


# ── DB Persistence ────────────────────────────────────────────────────

def save_entities(db_instance, entities: list[dict]):
    """
    Persist resolved entities to DB.
    Uses (query, platform) as the natural key to prevent duplicates
    across repeated searches for the same product.
    """
    for entity in entities:
        query             = entity["query"]
        canonical_name    = entity["canonical_name"]
        canonical_brand   = entity["canonical_brand"]
        canonical_variant = entity["canonical_variant"]

        for listing in entity["listings"]:
            platform    = listing.get("platform", "")
            price_value = listing.get("price", {}).get("value", 0)
            title       = listing.get("title") or canonical_name

            with db_instance._conn() as conn:
                # Find or create entity by (query, platform)
                existing = db_instance._fetchone(conn, """
                    SELECT el.entity_id
                    FROM entity_listings el
                    JOIN entities e ON e.id = el.entity_id
                    WHERE e.query = %s AND el.platform = %s
                    LIMIT 1
                """, (query, platform))

                if existing:
                    entity_id = str(existing["entity_id"])
                    # Update entity metadata
                    db_instance._execute(conn, """
                        UPDATE entities SET
                            canonical_name    = %s,
                            canonical_brand   = %s,
                            canonical_variant = %s,
                            updated_at        = NOW()
                        WHERE id = %s
                    """, (canonical_name, canonical_brand, canonical_variant, entity_id))
                else:
                    entity_id = entity["entity_id"]
                    # Insert new entity
                    db_instance._execute(conn, """
                        INSERT INTO entities
                            (id, canonical_name, canonical_brand, canonical_variant, query, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            canonical_name    = EXCLUDED.canonical_name,
                            canonical_brand   = EXCLUDED.canonical_brand,
                            canonical_variant = EXCLUDED.canonical_variant,
                            updated_at        = NOW()
                    """, (entity_id, canonical_name, canonical_brand, canonical_variant, query))

                # Upsert listing
                db_instance._execute(conn, """
                    INSERT INTO entity_listings
                        (entity_id, platform, product_url, title, price,
                         ingredients, manufacturer, marketed_by,
                         availability, last_seen)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (entity_id, platform) DO UPDATE SET
                        price        = EXCLUDED.price,
                        title        = EXCLUDED.title,
                        product_url  = EXCLUDED.product_url,
                        availability = EXCLUDED.availability,
                        last_seen    = NOW()
                """, (
                    entity_id,
                    platform,
                    listing.get("url", ""),
                    title,
                    price_value,
                    listing.get("ingredients", ""),
                    listing.get("manufacturer", ""),
                    listing.get("marketed_by", ""),
                    listing.get("availability", "unknown"),
                ))

def get_entities_for_query(db_instance, query: str) -> list[dict]:
    """Fetch resolved entities from DB for a given query."""
    with db_instance._conn() as conn:
        entity_rows = db_instance._fetchall(conn, """
            SELECT * FROM entities WHERE query = %s ORDER BY updated_at DESC
        """, (query,))

    result = []
    for entity in entity_rows:
        with db_instance._conn() as conn:
            listings = db_instance._fetchall(conn, """
                SELECT * FROM entity_listings
                WHERE entity_id = %s ORDER BY price ASC NULLS LAST
            """, (str(entity["id"]),))

        prices = [
            {"platform": l["platform"], "price": float(l["price"])}
            for l in listings if l.get("price") and float(l["price"]) > 0
        ]
        best_price  = prices[0]  if prices else None
        worst_price = prices[-1] if len(prices) > 1 else None

        price_spread = None
        if best_price and worst_price and best_price["price"] != worst_price["price"]:
            diff_pct = round(
                ((worst_price["price"] - best_price["price"]) / best_price["price"]) * 100, 2
            )
            price_spread = {
                "min":            best_price["price"],
                "max":            worst_price["price"],
                "diff_percent":   diff_pct,
                "cheapest":       best_price["platform"],
                "most_expensive": worst_price["platform"],
                "savings":        round(worst_price["price"] - best_price["price"], 2),
            }

        result.append({
            "entity_id":        str(entity["id"]),
            "canonical_name":   entity["canonical_name"],
            "canonical_brand":  entity["canonical_brand"],
            "canonical_variant": entity["canonical_variant"],
            "query":            entity["query"],
            "listings": [
                {
                    "platform":     l["platform"],
                    "title":        l["title"],
                    "price":        {"value": float(l["price"]) if l["price"] else 0, "currency": "INR"},
                    "url":          l["product_url"],
                    "ingredients":  l.get("ingredients", ""),
                    "manufacturer": l.get("manufacturer", ""),
                    "marketed_by":  l.get("marketed_by", ""),
                    "availability": l.get("availability", "unknown"),
                    "last_seen":    l["last_seen"].isoformat(),
                }
                for l in listings
            ],
            "best_price":    best_price,
            "price_spread":  price_spread,
            "platform_count": len(listings),
        })

    return result