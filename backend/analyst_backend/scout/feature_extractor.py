"""
scout/feature_extractor.py — Extract and normalize product + platform features
for comparison across platforms.

Works entirely from already-scraped page content stored in product_details JSONB.
No new scraping needed — reads from DB.

Output schema per listing:
{
    "platform": "amazon",
    "price": {"value": 183, "currency": "INR"},
    "url": "...",
    
    "platform_features": {
        "availability":    "in_stock",
        "seller":          "Cloudtail India",
        "return_policy":   "7 days",
        "shipping":        "Free delivery",
        "rating":          "4.3",
        "review_count":    "2847",
        "fulfilled_by":    "Amazon",
    },
    
    "product_features": {
        "brand":           "Organic India",
        "form":            "Capsule",
        "count":           "60",
        "dosage":          "400mg",
        "weight":          null,
        "key_ingredients": "Ashwagandha root 400mg",
        "manufacturer":    "ORGANIC INDIA Pvt. Ltd.",
        "marketed_by":     "ORGANIC INDIA Pvt. Ltd.",
        "country_of_origin": "India",
        "certifications":  "GMP, NPOP Certified",
        "flavor":          null,
        "size":            null,
        "color":           null,
        "material":        null,
        "storage":         null,
        "ram":             null,
        "battery":         null,
        "warranty":        null,
    }
}
"""

import os, re, json, logging
from openai import OpenAI
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# LAZY init — don't create at import time because load_dotenv() in main.py
# may not have run yet.
_openai_client = None

def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from .langfuse_config import get_openai_client
        _openai_client = get_openai_client()
    return _openai_client


# ── Category Detection ────────────────────────────────────────────────

CATEGORY_SIGNALS = {
    "health": [
        'capsule', 'tablet', 'supplement', 'vitamin', 'mineral',
        'protein', 'probiotic', 'ayurvedic', 'herbal', 'mg', 'mcg',
        'ashwagandha', 'omega', 'collagen', 'biotin', 'whey', 'powder',
        'softgel', 'gummy', 'effervescent', 'immunity', 'wellness',
    ],
    "electronics": [
        'gb', 'tb', 'ram', 'processor', 'display', 'battery', 'mah',
        'camera', 'mp', 'ghz', 'usb', 'hdmi', 'wifi', 'bluetooth',
        'laptop', 'phone', 'smartphone', 'tablet', 'monitor', 'tv',
        'speaker', 'headphone', 'earphone', 'charger', 'watt', 'hair dryer', 'straightner'
    ],
    "fashion": [
        'size', 'colour', 'color', 'fabric', 'material', 'fit',
        'sleeve', 'neck', 'waist', 'length', 'cotton', 'polyester',
        'leather', 'denim', 'wool', 'casual', 'formal', 'ethnic',
        'watch', 'dial', 'strap', 'band', 'case material',
    ],
    "grocery": [
        'weight', 'kg', 'grams', 'litre', 'ml', 'pack of',
        'vegetarian', 'vegan', 'gluten', 'organic', 'natural',
        'calories', 'protein', 'carbohydrate', 'fat', 'sodium',
        'expiry', 'shelf life', 'ingredients', 'preservatives',
    ],
    "beauty": [
        'spf', 'skin type', 'concern', 'serum', 'moisturizer',
        'sunscreen', 'foundation', 'lipstick', 'mascara', 'toner',
        'ml', 'oz', 'fragrance', 'paraben', 'sulfate', 'dermatologist',
    ],
}

def detect_category(title: str, details_text: str) -> str:
    """Detect product category from title and details text."""
    combined = (title + " " + details_text).lower()
    
    scores = {}
    for category, signals in CATEGORY_SIGNALS.items():
        scores[category] = sum(1 for s in signals if s in combined)
    
    best = max(scores, key=scores.get)
    return best if scores[best] >= 2 else "general"


# ── Rule-based Feature Extraction ────────────────────────────────────

def extract_features_rule_based(
    title: str,
    details: dict,
    category: str
) -> dict:
    """
    Extract features using regex patterns on already-scraped data.
    Works on product_details JSONB stored in DB — no new scraping.
    """
    # Combine all text sources from stored details
    text_parts = [title]
    for key in ["ingredients", "supplement_facts", "manufacturer", "marketed_by",
                 "availability", "description"]:
        val = details.get(key, "")
        if val and isinstance(val, str):
            text_parts.append(val)

    # Also include flattened specifications values
    specs = details.get("specifications") or {}
    if specs and isinstance(specs, dict):
        text_parts.append(" ".join(f"{k} {v}" for k, v in specs.items()))
    
    full_text = " ".join(text_parts).lower()
    
    features = {
        # Universal fields — every product
        "brand":            _extract_brand(title),
        "manufacturer":     _clean(details.get("manufacturer", "")),
        "marketed_by":      _clean(details.get("marketed_by", "")),
        "country_of_origin": _extract_country(full_text),
        "warranty":         _extract_warranty(full_text),

        # Health specific
        "form":             None,
        "count":            None,
        "dosage":           None,
        "key_ingredients":  None,
        "certifications":   None,
        "net_quantity":     None,

        # Electronics specific
        "storage":          None,
        "ram":              None,
        "display":          None,
        "battery":          None,
        "processor":        None,
        "camera":           None,
        "connectivity":     None,
        "color":            None,

        # Fashion specific
        "material":         None,
        "size":             None,
        "fit":              None,
        "occasion":         None,

        # Grocery/Beauty specific
        "weight":           None,
        "volume":           None,
        "flavor":           None,
        "skin_type":        None,
        "spf":              None,
    }

    if category == "health":
        features.update({
            "form":            _extract_form(full_text),
            "count":           _extract_count(full_text),
            "dosage":          _extract_dosage(full_text),
            "key_ingredients": _clean(details.get("ingredients", "")),
            "certifications":  _extract_certifications(full_text),
            "net_quantity":    _extract_net_quantity(full_text),
            "flavor":          _extract_flavor(full_text),
        })

    elif category == "electronics":
        features.update({
            "storage":      _extract_storage(full_text),
            "ram":          _extract_ram(full_text),
            "display":      _extract_display(full_text),
            "battery":      _extract_battery(full_text),
            "processor":    _extract_processor(full_text),
            "color":        _extract_color(title),
            "connectivity": _extract_connectivity(full_text),
        })

    elif category == "fashion":
        features.update({
            "material":  _extract_material(full_text),
            "color":     _extract_color(title),
            "size":      _extract_clothing_size(full_text),
            "fit":       _extract_fit(full_text),
            "occasion":  _extract_occasion(full_text),
        })

    elif category in ("grocery", "beauty"):
        features.update({
            "weight":    _extract_weight(full_text),
            "volume":    _extract_volume(full_text),
            "flavor":    _extract_flavor(full_text),
            "spf":       _extract_spf(full_text),
            "skin_type": _extract_skin_type(full_text),
        })

    # Remove None values for cleanliness — frontend handles missing
    return {k: v for k, v in features.items() if v is not None}


# ── Individual Extractors ─────────────────────────────────────────────

def _clean(s: str) -> str | None:
    if not s or s.strip() in ("Not available", "N/A", ""):
        return None
    return s.strip()

def _extract_brand(title: str) -> str | None:
    from .entity_resolver import extract_brand_from_title
    return extract_brand_from_title(title)

def _extract_country(text: str) -> str | None:
    match = re.search(
        r'country\s+of\s+origin[:\s]+([A-Za-z\s]+?)(?:\.|,|\n|$)',
        text, re.IGNORECASE
    )
    if match:
        return match.group(1).strip().title()
    if "made in india" in text or "manufactured in india" in text:
        return "India"
    if "made in usa" in text or "manufactured in usa" in text:
        return "USA"
    return None

def _extract_warranty(text: str) -> str | None:
    match = re.search(
        r'(\d+)\s*(?:year|month|day)s?\s*warranty',
        text, re.IGNORECASE
    )
    if match:
        return match.group(0).strip()
    return None

def _extract_form(text: str) -> str | None:
    FORMS = [
        'capsule', 'tablet', 'softgel', 'gummy', 'gummies',
        'powder', 'liquid', 'syrup', 'drops', 'spray',
        'effervescent', 'sachet', 'gel', 'cream', 'oil',
        'chewable', 'lozenge', 'patch',
    ]
    for form in FORMS:
        if form in text:
            return form.capitalize()
    return None

def _extract_count(text: str) -> str | None:
    match = re.search(
        r'(\d+)\s*(?:capsules?|tablets?|softgels?|gummies|sachets?|'
        r'strips?|patches?|pieces?|pcs|count|ct)\b',
        text, re.IGNORECASE
    )
    if match:
        return match.group(0).strip()
    return None

def _extract_dosage(text: str) -> str | None:
    match = re.search(
        r'(\d+(?:\.\d+)?)\s*(mg|mcg|iu|g)\b',
        text, re.IGNORECASE
    )
    if match:
        return match.group(0).strip()
    return None

def _extract_net_quantity(text: str) -> str | None:
    match = re.search(
        r'net\s+(?:quantity|weight|content)[:\s]+([^\n,\.]{3,30})',
        text, re.IGNORECASE
    )
    if match:
        return match.group(1).strip()
    return None

def _extract_certifications(text: str) -> str | None:
    certs = []
    CERT_PATTERNS = [
        (r'\bgmp\b',         'GMP'),
        (r'\bnpop\b',        'NPOP'),
        (r'\busda\s+organic','USDA Organic'),
        (r'\bfssai\b',       'FSSAI'),
        (r'\biso\s*\d+',     'ISO'),
        (r'\bhalal\b',       'Halal'),
        (r'\bkosher\b',      'Kosher'),
        (r'\bvegan\b',       'Vegan'),
        (r'\bvegetarian\b',  'Vegetarian'),
        (r'\bnon.?gmo\b',    'Non-GMO'),
        (r'\bgluten.?free\b','Gluten-Free'),
        (r'\borganic\b',     'Organic'),
    ]
    for pattern, label in CERT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            certs.append(label)
    return ", ".join(certs) if certs else None

def _extract_flavor(text: str) -> str | None:
    FLAVORS = [
        'chocolate', 'vanilla', 'strawberry', 'mango', 'orange',
        'lemon', 'mint', 'unflavored', 'natural', 'tropical',
        'berry', 'apple', 'watermelon', 'pineapple', 'coffee',
    ]
    for flavor in FLAVORS:
        if flavor in text:
            return flavor.capitalize()
    return None

def _extract_storage(text: str) -> str | None:
    match = re.search(r'(\d+)\s*(tb|gb)\b(?!\s*ram)', text, re.IGNORECASE)
    if match:
        return match.group(0).strip().upper()
    return None

def _extract_ram(text: str) -> str | None:
    match = re.search(r'(\d+)\s*gb\s*ram', text, re.IGNORECASE)
    if match:
        return match.group(0).strip().upper()
    return None

def _extract_display(text: str) -> str | None:
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:inch|")\s*(?:display|screen)?', text, re.IGNORECASE)
    if match:
        size  = match.group(1)
        res_match = re.search(r'(4k|2k|fhd|hd|qhd|uhd|1080p|720p)', text, re.IGNORECASE)
        res   = res_match.group(1).upper() if res_match else ""
        return f"{size} inch {res}".strip()
    return None

def _extract_battery(text: str) -> str | None:
    match = re.search(r'(\d{3,5})\s*mah', text, re.IGNORECASE)
    if match:
        return f"{match.group(1)} mAh"
    return None

def _extract_processor(text: str) -> str | None:
    patterns = [
        r'(snapdragon\s+\d+\w*)',
        r'(apple\s+[am]\d+\w*)',
        r'(mediatek\s+\w+)',
        r'(dimensity\s+\d+\w*)',
        r'(intel\s+core\s+i\d[\w\-]*)',
        r'(amd\s+ryzen\s+\d\s+\w+)',
        r'(exynos\s+\d+)',
    ]
    for p in patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            return match.group(1).strip().title()
    return None

def _extract_color(title: str) -> str | None:
    COLORS = [
        'black', 'white', 'silver', 'gold', 'blue', 'red',
        'green', 'pink', 'grey', 'gray', 'purple', 'yellow',
        'orange', 'brown', 'beige', 'navy', 'titanium',
        'midnight', 'starlight', 'coral', 'space gray',
        'rose gold', 'champagne', 'graphite',
    ]
    title_lower = title.lower()
    for color in COLORS:
        if color in title_lower:
            return color.title()
    return None

def _extract_connectivity(text: str) -> str | None:
    features = []
    if re.search(r'5g', text, re.IGNORECASE):         features.append("5G")
    if re.search(r'4g|lte', text, re.IGNORECASE):     features.append("4G")
    if re.search(r'wifi|wi-fi', text, re.IGNORECASE): features.append("WiFi")
    if re.search(r'bluetooth', text, re.IGNORECASE):  features.append("Bluetooth")
    if re.search(r'nfc', text, re.IGNORECASE):        features.append("NFC")
    if re.search(r'usb.?c', text, re.IGNORECASE):     features.append("USB-C")
    return ", ".join(features) if features else None

def _extract_material(text: str) -> str | None:
    MATERIALS = [
        'cotton', 'polyester', 'nylon', 'wool', 'silk', 'linen',
        'leather', 'synthetic', 'denim', 'rayon', 'spandex',
        'viscose', 'acrylic', 'fleece', 'canvas', 'suede',
        'stainless steel', 'aluminium', 'titanium', 'plastic',
    ]
    for material in MATERIALS:
        if material in text:
            return material.title()
    return None

def _extract_clothing_size(text: str) -> str | None:
    match = re.search(
        r'\b(xs|s|m|l|xl|xxl|xxxl|2xl|3xl|free\s*size)\b',
        text, re.IGNORECASE
    )
    if match:
        return match.group(1).upper()
    return None

def _extract_fit(text: str) -> str | None:
    FITS = ['slim fit', 'regular fit', 'loose fit', 'relaxed fit',
            'skinny', 'straight fit', 'oversized']
    for fit in FITS:
        if fit in text:
            return fit.title()
    return None

def _extract_occasion(text: str) -> str | None:
    OCCASIONS = ['casual', 'formal', 'party', 'sports', 'ethnic',
                 'wedding', 'office', 'beach', 'festival']
    for occ in OCCASIONS:
        if occ in text:
            return occ.title()
    return None

def _extract_weight(text: str) -> str | None:
    match = re.search(r'(\d+(?:\.\d+)?)\s*(kg|g|gm|gms)\b', text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return None

def _extract_volume(text: str) -> str | None:
    match = re.search(r'(\d+(?:\.\d+)?)\s*(ml|l|litre|liter|fl\s*oz)\b', text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return None

def _extract_spf(text: str) -> str | None:
    match = re.search(r'spf\s*(\d+)', text, re.IGNORECASE)
    if match:
        return f"SPF {match.group(1)}"
    return None

def _extract_skin_type(text: str) -> str | None:
    SKIN_TYPES = ['oily', 'dry', 'combination', 'sensitive',
                  'normal', 'all skin types']
    for skin in SKIN_TYPES:
        if skin in text:
            return skin.title()
    return None


# ── LLM Gap Filler ────────────────────────────────────────────────────

def llm_fill_missing_features(
    title: str,
    category: str,
    extracted: dict,
    raw_text: str
) -> dict:
    """
    Use LLM to fill in features that rule-based extraction missed.
    Only called when there are significant gaps.
    Sends already-scraped text — no new HTTP requests.
    """
    if not _get_openai_client():
        return extracted

    # Only call LLM if at least 3 important fields are missing
    important_fields = {
        "health":      ["form", "count", "dosage", "key_ingredients", "certifications"],
        "electronics": ["storage", "ram", "display", "battery", "processor", "color"],
        "fashion":     ["material", "color", "size", "fit"],
        "grocery":     ["weight", "flavor"],
        "beauty":      ["volume", "spf", "skin_type"],
        "general":     ["brand", "country_of_origin"],
    }

    fields_to_check = important_fields.get(category, important_fields["general"])
    missing = [f for f in fields_to_check if not extracted.get(f)]

    if len(missing) < 2:
        # Enough extracted by rules — skip LLM
        return extracted

    logger.info(f"LLM gap-fill for [{category}]: missing {missing}")

    try:
        response = _get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            # model="gemini-2.5-flash",
            temperature=0,
            max_tokens=400,
            messages=[
                {
                    "role": "system",
                    "content": f"""Extract product features from the given product page text.
Product category: {category}
Fields to extract: {', '.join(missing)}

Return ONLY a JSON object with the requested fields.
If a field cannot be determined from the text, use null.
Do not guess or infer — only extract what is explicitly stated.
Return ONLY valid JSON, no markdown, no explanation."""
                },
                {
                    "role": "user",
                    "content": f"Product: {title}\n\nPage text:\n{raw_text[:3000]}"
                }
            ]
        )

        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        llm_data = json.loads(raw)

        # Merge — only fill fields that rules missed
        for field, value in llm_data.items():
            if value and not extracted.get(field):
                extracted[field] = value
                logger.info(f"  LLM filled: {field} = {value}")

        return extracted

    except Exception as e:
        logger.warning(f"LLM gap-fill failed: {e}")
        return extracted


# ── Platform Feature Extraction ───────────────────────────────────────

def extract_platform_features(details: dict) -> dict:
    """
    Extract platform-specific features from stored product_details.
    These are platform behaviors, not product specs.
    Accepts the product_details nested dict directly.
    """
    # Flatten specs into details for text scanning
    specs = details.get("specifications") or {}
    all_data = {**details, **specs}
    text = json.dumps(all_data).lower()

    features = {
        "availability": details.get("availability", "unknown"),
        "seller":       None,
        "fulfilled_by": None,
        "return_policy": None,
        "shipping":     None,
        "rating":       None,
        "review_count": None,
        "offer":        None,
    }

    # Seller
    for key in ["sold_by", "seller", "vendor", "store"]:
        val = details.get(key)
        if val:
            features["seller"] = val
            break

    # Return policy
    ret = re.search(r'(\d+)\s*(?:day|days)\s*(?:return|replacement)', text)
    if ret:
        features["return_policy"] = f"{ret.group(1)} days"

    # Rating
    rating = re.search(r'(\d+\.\d+)\s*(?:out of|\/)\s*5', text)
    if rating:
        features["rating"] = rating.group(1)

    # Review count
    reviews = re.search(r'([\d,]+)\s*(?:ratings?|reviews?)', text)
    if reviews:
        features["review_count"] = reviews.group(1).replace(",", "")

    # Shipping
    if "free delivery" in text or "free shipping" in text:
        features["shipping"] = "Free"
    elif "express" in text or "same day" in text:
        features["shipping"] = "Express"

    # Offers
    offer = re.search(r'(\d+)%\s*off', text)
    if offer:
        features["offer"] = f"{offer.group(1)}% off"

    return {k: v for k, v in features.items() if v is not None}


# ── Main Entry Point ──────────────────────────────────────────────────

def compare_features(product_name: str, listings: list[dict]) -> dict:
    """
    Main entry point — given a product name and its listings from multiple
    platforms, extract and normalize all features for comparison.

    Input: listings from DB (each has platform, price, url, product_details)
    Output: structured comparison JSON ready for frontend
    """
    if not listings:
        return {"product": product_name, "platforms": [], "feature_matrix": {}}

    # Detect category from first listing with content
    category = "general"
    for listing in listings:
        details = listing.get("product_details") or listing
        title   = listing.get("title", product_name)
        details_text = json.dumps(details)
        detected = detect_category(title, details_text)
        if detected != "general":
            category = detected
            break

    logger.info(f"Feature comparison: '{product_name}' → category={category}, {len(listings)} platforms")

    # Extract features per platform
    platform_results = []

    for listing in listings:
        platform = listing.get("platform", "unknown")
        title    = listing.get("title", product_name)

        # product_details is always the nested dict now
        details  = listing.get("product_details") or {}
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}

        # Flatten nested specifications into details for rule-based extraction
        specs = details.get("specifications") or {}
        merged = {**details, **specs}

        # Also pull top-level listing fields as fallback (old cache format)
        for key in ["ingredients", "manufacturer", "marketed_by", "availability"]:
            if listing.get(key) and not merged.get(key):
                merged[key] = listing[key]

        # Build raw text for LLM gap-filler — include description and specs
        raw_text = " ".join(filter(None, [
            title,
            merged.get("description", ""),
            merged.get("ingredients", ""),
            merged.get("supplement_facts", ""),
            merged.get("manufacturer", ""),
            merged.get("marketed_by", ""),
            " ".join(f"{k}: {v}" for k, v in specs.items())[:1000],
            json.dumps(merged)[:1500],
        ]))

        # Stage 1: Rule-based extraction
        product_feats = extract_features_rule_based(title, merged, category)

        # Stage 2: LLM fills gaps
        product_feats = llm_fill_missing_features(title, category, product_feats, raw_text)

        # Platform features
        platform_feats = extract_platform_features(merged)

        platform_results.append({
            "platform":          platform,
            "price":             listing.get("price", {"value": 0, "currency": "INR"}),
            "url":               listing.get("url", ""),
            "last_updated":      listing.get("last_seen", ""),
            "platform_features": platform_feats,
            "product_features":  product_feats,
        })

    # Build feature matrix — all unique feature keys across all platforms
    all_product_keys  = set()
    all_platform_keys = set()

    for r in platform_results:
        all_product_keys.update(r["product_features"].keys())
        all_platform_keys.update(r["platform_features"].keys())

    # Build matrix: {feature_name: {platform: value}}
    product_matrix  = {}
    platform_matrix = {}

    for key in sorted(all_product_keys):
        product_matrix[key] = {}
        for r in platform_results:
            product_matrix[key][r["platform"]] = r["product_features"].get(key)

    for key in sorted(all_platform_keys):
        platform_matrix[key] = {}
        for r in platform_results:
            platform_matrix[key][r["platform"]] = r["platform_features"].get(key)

    # Identify where platforms AGREE vs DIFFER on product features
    agreement = {}
    for key, platform_vals in product_matrix.items():
        vals = [v for v in platform_vals.values() if v is not None]
        agreement[key] = "agree" if len(set(vals)) <= 1 else "differ"

    return {
        "product":          product_name,
        "category":         category,
        "platforms":        [r["platform"] for r in platform_results],
        "platform_details": platform_results,
        "feature_matrix": {
            "product_features":  product_matrix,
            "platform_features": platform_matrix,
            "agreement":         agreement,
        },
        "summary": {
            "total_features_extracted": len(all_product_keys) + len(all_platform_keys),
            "product_feature_count":    len(all_product_keys),
            "platform_feature_count":   len(all_platform_keys),
            "features_that_differ":     [k for k, v in agreement.items() if v == "differ"],
            "features_that_agree":      [k for k, v in agreement.items() if v == "agree"],
        }
    }