"""
sentiment.py — Automatic Sentiment Analysis for Customer Reviews
=================================================================
Reads customer reviews from the database, analyzes the review_text
using NLP, and updates the sentiment column automatically.

HOW IT WORKS:
─────────────
1. Fetches all reviews for a client where sentiment is NULL or empty
2. For each review_text, runs sentiment analysis to classify as:
   - "positive"  (compound score > 0.05)
   - "negative"  (compound score < -0.05)
   - "neutral"   (between -0.05 and 0.05)
3. Updates the sentiment column in the database
4. Also analyzes reviews that already have sentiment (to verify/correct)

SENTIMENT ENGINES (in priority order):
──────────────────────────────────────
1. VADER (vaderSentiment) — Best for short reviews, social media text
   pip install vaderSentiment
2. TextBlob — Good general-purpose sentiment
   pip install textblob
3. Keyword-based fallback — Simple word matching (no install needed)

USAGE:
  # From command line:
  python -m ml.sentiment --db-url postgresql://... --client-id CLT-002

  # From pipeline (called automatically):
  python -m ml.sentiment --db-url postgresql://... --client-id CLT-002 --update-all
"""

import os
import sys
import re
import logging
import argparse
from typing import Optional, Tuple

from dotenv import load_dotenv

log = logging.getLogger("sentiment")
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: SENTIMENT ENGINES
# ═══════════════════════════════════════════════════════════════════════════

class VaderEngine:
    """
    VADER (Valence Aware Dictionary and sEntiment Reasoner)
    Best for: short texts, reviews, social media posts
    It understands things like:
    - "GREAT!!!" (capitalization + punctuation = stronger)
    - "not bad" (negation handling)
    - Emojis and slang
    """
    def __init__(self):
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        self.analyzer = SentimentIntensityAnalyzer()
        log.info("  Using VADER sentiment engine")

    def analyze(self, text: str) -> Tuple[str, float]:
        """Returns (label, compound_score)"""
        scores = self.analyzer.polarity_scores(text)
        compound = scores['compound']
        if compound >= 0.05:
            return "positive", compound
        elif compound <= -0.05:
            return "negative", compound
        else:
            return "neutral", compound


class TextBlobEngine:
    """
    TextBlob — general-purpose NLP library.
    Uses pattern-based sentiment analysis.
    Polarity ranges from -1 (negative) to +1 (positive).
    """
    def __init__(self):
        from textblob import TextBlob
        self._TextBlob = TextBlob
        log.info("  Using TextBlob sentiment engine")

    def analyze(self, text: str) -> Tuple[str, float]:
        blob = self._TextBlob(text)
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return "positive", polarity
        elif polarity < -0.1:
            return "negative", polarity
        else:
            return "neutral", polarity


class KeywordEngine:
    """
    Simple keyword-based fallback — no external libraries needed.
    Counts positive and negative words in the review text.
    Not as accurate as VADER/TextBlob, but works without any pip install.
    """
    POSITIVE_WORDS = {
        'love', 'great', 'excellent', 'amazing', 'awesome', 'fantastic',
        'wonderful', 'perfect', 'best', 'good', 'happy', 'satisfied',
        'recommend', 'quality', 'fresh', 'fast', 'helpful', 'friendly',
        'delicious', 'beautiful', 'comfortable', 'reliable', 'impressive',
        'outstanding', 'superb', 'brilliant', 'pleased', 'enjoy', 'favorite',
        'worth', 'convenient', 'smooth', 'efficient', 'sturdy', 'durable',
    }
    NEGATIVE_WORDS = {
        'bad', 'terrible', 'horrible', 'awful', 'worst', 'poor', 'hate',
        'disappointed', 'disappointing', 'broken', 'defective', 'waste',
        'overpriced', 'slow', 'rude', 'damaged', 'missing', 'wrong',
        'complaint', 'refund', 'return', 'expired', 'stale', 'cheap',
        'flimsy', 'uncomfortable', 'unreliable', 'frustrating', 'useless',
        'disgusting', 'nasty', 'annoying', 'regret', 'avoid', 'never',
    }

    def __init__(self):
        log.info("  Using keyword-based sentiment engine (fallback)")

    def analyze(self, text: str) -> Tuple[str, float]:
        words = set(re.findall(r'\w+', text.lower()))
        pos_count = len(words & self.POSITIVE_WORDS)
        neg_count = len(words & self.NEGATIVE_WORDS)
        total = pos_count + neg_count

        if total == 0:
            return "neutral", 0.0

        score = (pos_count - neg_count) / total
        if score > 0.2:
            return "positive", score
        elif score < -0.2:
            return "negative", score
        else:
            return "neutral", score


def get_best_engine():
    """
    Try to load the best available sentiment engine.
    Falls back gracefully if libraries aren't installed.
    """
    # Try VADER first (best for reviews)
    try:
        return VaderEngine()
    except ImportError:
        pass

    # Try TextBlob
    try:
        return TextBlobEngine()
    except ImportError:
        pass

    # Fallback to keywords
    return KeywordEngine()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_reviews(db_url: str, client_id: str, update_all: bool = False) -> dict:
    """
    Main function: analyze customer reviews and update sentiment in database.

    Args:
        db_url: PostgreSQL connection string
        client_id: Which client's reviews to analyze (e.g., 'CLT-002')
        update_all: If True, re-analyze ALL reviews (even those with sentiment).
                    If False, only analyze reviews where sentiment is NULL.

    Returns:
        Dict with stats: total_analyzed, positive, negative, neutral counts
    """
    from sqlalchemy import create_engine, text

    log.info("=" * 60)
    log.info("  SENTIMENT ANALYSIS — %s", client_id)
    log.info("=" * 60)

    # 1. Get the best available sentiment engine
    engine_nlp = get_best_engine()

    # 2. Connect to database
    engine = create_engine(db_url, pool_pre_ping=True)
    log.info("  Connected to database")

    # 3. Fetch reviews that need analysis
    with engine.connect() as conn:
        if update_all:
            # Re-analyze everything
            rows = conn.execute(
                text("""
                    SELECT review_id, review_text, rating
                    FROM customer_reviews
                    WHERE client_id = :cid
                      AND review_text IS NOT NULL
                      AND review_text != ''
                """),
                {"cid": client_id},
            ).fetchall()
            log.info("  Fetched %d reviews (update_all=True)", len(rows))
        else:
            # Only analyze reviews without sentiment
            rows = conn.execute(
                text("""
                    SELECT review_id, review_text, rating
                    FROM customer_reviews
                    WHERE client_id = :cid
                      AND review_text IS NOT NULL
                      AND review_text != ''
                      AND (sentiment IS NULL OR sentiment = '')
                """),
                {"cid": client_id},
            ).fetchall()
            log.info("  Fetched %d reviews needing sentiment analysis", len(rows))

    if not rows:
        log.info("  No reviews to analyze — all already have sentiment labels")
        return {"total_analyzed": 0, "positive": 0, "negative": 0, "neutral": 0}

    # 4. Analyze each review
    results = {"positive": 0, "negative": 0, "neutral": 0}
    updates = []

    for review_id, review_text, rating in rows:
        if not review_text or not review_text.strip():
            continue

        # Run NLP sentiment analysis on the text
        label, score = engine_nlp.analyze(review_text)

        # BONUS: Cross-check with star rating for better accuracy
        # If rating is 1-2 stars but text says "positive", trust the rating more
        # If rating is 4-5 stars but text says "negative", trust the rating more
        if rating is not None:
            if rating <= 2 and label == "positive":
                label = "negative"  # Low stars override positive text
            elif rating >= 4 and label == "negative":
                label = "positive"  # High stars override negative text

        results[label] += 1
        updates.append({"rid": review_id, "cid": client_id, "sentiment": label, "score": score})

    # 5. Batch update sentiment in database
    if updates:
        with engine.connect() as conn:
            # Update in batches of 500
            batch_size = 500
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                for u in batch:
                    conn.execute(
                        text("""
                            UPDATE customer_reviews
                            SET sentiment = :sentiment, sentiment_score = :score
                            WHERE client_id = :cid AND review_id = :rid
                        """),
                        u,
                    )
                conn.commit()

        log.info("  Updated %d reviews in database", len(updates))

    total = len(updates)
    log.info("  Results:")
    log.info("    Positive: %d (%.1f%%)", results['positive'],
             results['positive'] / total * 100 if total > 0 else 0)
    log.info("    Negative: %d (%.1f%%)", results['negative'],
             results['negative'] / total * 100 if total > 0 else 0)
    log.info("    Neutral:  %d (%.1f%%)", results['neutral'],
             results['neutral'] / total * 100 if total > 0 else 0)
    log.info("=" * 60)

    engine.dispose()

    return {
        "total_analyzed": total,
        "positive": results["positive"],
        "negative": results["negative"],
        "neutral": results["neutral"],
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Analyze customer review sentiment using NLP"
    )
    parser.add_argument("--db-url", type=str, default=os.getenv("DB_URL"),
                        help="PostgreSQL connection string")
    parser.add_argument("--client-id", type=str, required=True,
                        help="Client ID to analyze (e.g., CLT-002)")
    parser.add_argument("--update-all", action="store_true",
                        help="Re-analyze ALL reviews, not just ones missing sentiment")
    args = parser.parse_args()

    if not args.db_url:
        log.error("DB_URL not set. Use --db-url or set DB_URL env var.")
        sys.exit(1)

    result = analyze_reviews(args.db_url, args.client_id, update_all=args.update_all)

    if result["total_analyzed"] == 0:
        print("No reviews needed analysis.")
    else:
        print(f"Analyzed {result['total_analyzed']} reviews: "
              f"{result['positive']} positive, "
              f"{result['negative']} negative, "
              f"{result['neutral']} neutral")


if __name__ == "__main__":
    main()
