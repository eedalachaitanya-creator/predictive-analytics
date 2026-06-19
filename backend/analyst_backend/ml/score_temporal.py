"""Score CURRENT customers with the trained temporal model and write the result
into the live ``churn_scores`` table — the step that makes temporal (forward
90-day) predictions show up in the dashboard.

ADDITIVE. This module is the bridge between the temporal training path
(``ml.train_temporal`` → a bundle under ``ml/models/temporal/``) and the table
the dashboard reads. It:

  1. builds a point-in-time (<=T) snapshot at T = the tenant's latest order date,
     covering ALL eligible customers (``active_only=False``) so dashboard
     coverage matches the live model — the forward label is irrelevant here and
     is ignored; only the <=T features are used;
  2. aligns those features to the bundle's feature list (imputing any missing
     one with the training median), applies the saved scaler, and runs
     ``predict_proba``;
  3. maps probability → risk tier with the SAME per-client thresholds the live
     scorer uses, and persists via ``ml.predict.save_scores_to_db`` (so no
     dashboard/frontend change is needed).

The estimator is loaded from a NON-DISCOVERABLE path; ``ml.predict`` still globs
only ``ml/models/*.joblib`` (the legacy model), so the live fallback is intact.
"""
from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("ml.score_temporal")


# ──────────────────────────────────────────────────────────────────────────────
# Feature alignment + scoring (pure; no DB)
# ──────────────────────────────────────────────────────────────────────────────

def align_features(
    df: pd.DataFrame,
    feature_names: Sequence[str],
    train_medians: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    """Reindex ``df`` to exactly ``feature_names`` (order preserved).

    A feature present in the bundle but missing from the scoring frame is
    imputed with its TRAINING median (the same value training used), falling
    back to 0.0 when no median is recorded. Extra columns in ``df`` are dropped.
    """
    train_medians = train_medians or {}
    out = pd.DataFrame(index=df.index)
    for f in feature_names:
        if f in df.columns:
            col = pd.to_numeric(df[f], errors="coerce")
        else:
            col = pd.Series(np.nan, index=df.index, dtype="float64")
        fill = train_medians.get(f)
        out[f] = col.fillna(0.0 if fill is None else float(fill))
    return out


def score_frame(df: pd.DataFrame, bundle: Mapping) -> Tuple[List, np.ndarray]:
    """Score a wide snapshot frame with a temporal bundle.

    ``bundle`` is the ``{model, scaler, feature_names, metadata}`` dict written by
    ``ml.train_model.save_model``. Returns ``(customer_ids, probabilities)`` where
    ``probabilities`` is P(churn in the next label window), aligned row-for-row to
    ``customer_ids``.
    """
    model = bundle["model"]
    scaler = bundle.get("scaler")
    feature_names = list(bundle["feature_names"])
    metadata = bundle.get("metadata") or {}
    train_medians = metadata.get("train_medians") or {}

    customer_ids = df["customer_id"].tolist()
    X = align_features(df, feature_names, train_medians)
    X_mat = scaler.transform(X) if scaler is not None else X.to_numpy()
    proba = model.predict_proba(X_mat)[:, 1]
    return customer_ids, np.asarray(proba, dtype=float)


# ──────────────────────────────────────────────────────────────────────────────
# Stubs — driven GREEN by the next TDD slices
# ──────────────────────────────────────────────────────────────────────────────

def to_score_df(
    client_id: str,
    customer_ids: Sequence,
    probabilities: Sequence[float],
    *,
    thresholds: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    """Shape ``(client_id, customer_ids, probabilities)`` into the DataFrame
    ``ml.predict.save_scores_to_db`` expects.

    Columns: ``client_id, customer_id, churn_probability, risk_level`` — the
    risk tier is assigned with the SAME ``assign_risk_levels`` mapping (and the
    same per-client thresholds) the live scorer uses, so a temporal-scored row
    is indistinguishable in shape from a legacy-scored one. Full probability
    precision is preserved (the display layer rounds, not us).
    """
    from ml.predict import assign_risk_levels  # local import: avoid import-time cost

    probabilities = np.asarray(probabilities, dtype=float)
    risk_levels = assign_risk_levels(probabilities, thresholds)
    return pd.DataFrame({
        "client_id": client_id,
        "customer_id": list(customer_ids),
        "churn_probability": probabilities,
        "risk_level": risk_levels,
    })


def _resolve_max_order_date(engine_or_conn: Any, client_id: str) -> Optional[dt.date]:
    """Latest qualifying ``order_date`` for the tenant — the natural scoring T.

    Mirrors ``build_snapshot``'s engine-or-connection heuristic so a caller can
    inject an open connection (tests) or pass an Engine (the pipeline).
    """
    from sqlalchemy import text

    sql = text("SELECT MAX(order_date)::date FROM orders WHERE client_id = :c")
    if hasattr(engine_or_conn, "begin") and not hasattr(engine_or_conn, "execute"):
        with engine_or_conn.connect() as cx:
            return cx.execute(sql, {"c": client_id}).scalar()
    return engine_or_conn.execute(sql, {"c": client_id}).scalar()


def _resolve_scoring_asof(engine_or_conn: Any, client_id: str):
    """Scoring T = the latest REAL data MOMENT for the tenant — the newest of the
    last order, the last support ticket, or the last review, as a full timestamp.

    Anchored to actual data (never wall-clock ``today``) so features stay in the
    training distribution, but signals that arrive AFTER the last order (e.g. a
    Jira ticket raised this afternoon) still advance T so they fall inside the
    ``opened_date <= T`` snapshot. Returning a *timestamp* (not a date → midnight)
    is what makes a same-day-but-later ticket count. ``CAST(:T AS date)`` downstream
    keeps ``cutoff_date`` a clean date. ``GREATEST`` ignores NULLs, so missing
    signal tables degrade to the order date — identical to the old behavior.
    """
    from sqlalchemy import text

    sql = text("""
        SELECT GREATEST(
            (SELECT MAX(order_date)::timestamptz  FROM orders          WHERE client_id = :c),
            (SELECT MAX(opened_date)::timestamptz FROM support_tickets WHERE client_id = :c),
            (SELECT MAX(review_date)::timestamptz FROM customer_reviews WHERE client_id = :c)
        )
    """)
    if hasattr(engine_or_conn, "begin") and not hasattr(engine_or_conn, "execute"):
        with engine_or_conn.connect() as cx:
            return cx.execute(sql, {"c": client_id}).scalar()
    return engine_or_conn.execute(sql, {"c": client_id}).scalar()


def build_scoring_frame(
    engine_or_conn: Any,
    client_id: str,
    *,
    T: Optional[dt.date] = None,
    label_window_days: int = 90,
    min_tenure_days: int = 0,
    min_orders: int = 1,
    active_window_days: int = 120,
) -> pd.DataFrame:
    """Build the point-in-time (<=T) SCORING snapshot for a tenant.

    Defaults to T = the tenant's latest order date and ``active_only=False`` with
    relaxed eligibility (``min_orders=1``, ``min_tenure_days=0``) so coverage
    matches the live scorer — every customer with at least one qualifying order
    gets a forward-looking probability, not just the narrow active-at-T training
    cohort. The forward label the snapshot also computes is meaningless at the
    latest T (no observed future) and is ignored by ``score_frame``.
    """
    from ml.temporal_dataset import build_snapshot

    if T is None:
        T = _resolve_scoring_asof(engine_or_conn, client_id)
        if T is None:
            logger.warning("build_scoring_frame: no orders for client_id=%s", client_id)
            return pd.DataFrame()

    return build_snapshot(
        engine_or_conn, client_id, T,
        label_window_days=label_window_days,
        min_tenure_days=min_tenure_days,
        min_orders=min_orders,
        active_window_days=active_window_days,
        active_only=False,
    )


def compute_drivers(
    df: pd.DataFrame,
    bundle: Mapping,
    probabilities: Sequence[float],
    *,
    low_risk_cutoff: float = 0.35,
    top_n: int = 3,
) -> pd.DataFrame:
    """Per-customer top-N churn drivers for the dashboard's "Top Driver" column.

    Reuses the legacy ``ml.predict.compute_churn_drivers`` (signed SHAP attribution
    that already unwraps ``CalibratedClassifierCV``), so a temporal-scored row gets
    the SAME driver semantics as a legacy-scored one. Drivers are suppressed (None)
    below ``low_risk_cutoff`` — the UI shows no attribution for low-risk customers.
    Returns a DataFrame with ``driver_1 … driver_N``.
    """
    from ml.predict import compute_churn_drivers

    feature_names = list(bundle["feature_names"])
    metadata = bundle.get("metadata") or {}
    train_medians = metadata.get("train_medians") or {}
    scaler = bundle.get("scaler")

    X = align_features(df, feature_names, train_medians)
    X_scaled = pd.DataFrame(
        scaler.transform(X) if scaler is not None else X.to_numpy(),
        columns=feature_names,
    )
    return compute_churn_drivers(
        bundle["model"], X_scaled, top_n=top_n,
        probabilities=np.asarray(probabilities, dtype=float),
        low_risk_cutoff=low_risk_cutoff,
    )


def load_bundle(bundle_path: Any) -> Dict:
    """Load a temporal model bundle ({model, scaler, feature_names, metadata}).

    SECURITY: joblib/pickle deserialization executes arbitrary code, so it is
    only safe on TRUSTED inputs. The bundle here is a self-produced local
    artifact — written by this codebase's own ``ml.train_temporal`` /
    ``ml.train_model.save_model`` into ``ml/models/temporal/`` during the
    pipeline run, never fetched from an external/untrusted source. This matches
    the existing live loader (``ml/predict.py`` uses ``joblib.load`` the same
    way). Do NOT point this at a bundle from an untrusted origin.
    """
    import joblib

    return joblib.load(str(bundle_path))


def model_version_tag(bundle: Mapping) -> str:
    """A traceable ``churn_scores.model_version`` string (<=80 chars).

    Shape: ``temporal_<model_type>_<YYYY-MM-DD>_pr<pr_auc>`` so an auditor can see
    at a glance which trained temporal model produced a dashboard row, and that
    it came from the temporal (not legacy) path.
    """
    meta = bundle.get("metadata") or {}
    mtype = str(meta.get("model_type", "model"))
    trained = str(meta.get("trained_at", ""))[:10] or "unknown"
    pr = (meta.get("metrics") or {}).get("pr_auc")
    pr_str = f"_pr{float(pr):.3f}" if isinstance(pr, (int, float)) else ""
    return f"temporal_{mtype}_{trained}{pr_str}"[:80]


def score(
    engine_or_conn: Any,
    client_id: str,
    *,
    db_url: Optional[str] = None,
    bundle: Optional[Mapping] = None,
    bundle_path: Any = None,
    T: Optional[dt.date] = None,
    write: bool = True,
    model_version: Optional[str] = None,
    thresholds: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    """Score current customers with the temporal model → ``churn_scores`` shape.

    Pipeline: ``build_scoring_frame`` (point-in-time <=T) → ``score_frame``
    (align + predict_proba) → ``to_score_df`` (risk tiers). When ``write`` is
    True, persists via ``ml.predict.save_scores_to_db`` (same writer the live
    scorer uses), tagged with a temporal ``model_version``. Returns the score
    DataFrame (empty if the tenant has no scoreable customers).
    """
    if write and not db_url:
        raise ValueError("score: write=True requires db_url")
    if bundle is None:
        if bundle_path is None:
            raise ValueError("score: provide bundle or bundle_path")
        bundle = load_bundle(bundle_path)

    frame = build_scoring_frame(engine_or_conn, client_id, T=T)
    if frame.empty:
        logger.warning("score: no scoreable customers for client_id=%s", client_id)
        return pd.DataFrame()

    customer_ids, probabilities = score_frame(frame, bundle)

    if thresholds is None and db_url:
        from ml.predict import load_risk_thresholds
        thresholds = load_risk_thresholds(db_url, client_id)
    score_df = to_score_df(client_id, customer_ids, probabilities, thresholds=thresholds)

    # Per-customer churn drivers (dashboard "Top Driver" / Driver 1-3). Suppressed
    # below the MEDIUM cutoff. Wrapped so a driver-attribution failure can never
    # break scoring — scores still persist, just without drivers (graceful).
    cutoff = (thresholds or {}).get("medium", 0.35)
    try:
        drivers = compute_drivers(frame, bundle, probabilities, low_risk_cutoff=cutoff)
        for col in drivers.columns:
            score_df[col] = drivers[col].to_numpy()
    except Exception as exc:  # noqa: BLE001 — drivers are best-effort
        logger.warning("score: driver computation failed (%s) — writing scores "
                       "without drivers", exc)

    if write:
        from ml.predict import save_scores_to_db
        mv = model_version or model_version_tag(bundle)
        n = save_scores_to_db(score_df, db_url, model_version=mv)
        logger.info("score: wrote %s temporal rows to churn_scores (client_id=%s, %s)",
                    n, client_id, mv)
    return score_df


def export_scores_to_disk(
    engine_or_conn: Any,
    client_id: str,
    *,
    output_dir: Any = None,
) -> Tuple[Optional[Path], Optional[Path]]:
    """Regenerate the downloadable ``churn_scores.{csv,json}`` from the
    authoritative ``churn_scores`` table — keeping the Downloads page in
    lock-step with the dashboard.

    WHY THIS EXISTS: the live pipeline's legacy Stage-7 (``ml.predict``) writes
    the on-disk ``churn_scores.{csv,json}``; the temporal Stage-8 only overwrites
    the DB TABLE. Stage-12 (``save_all_output_files``) then copies the DISK files
    into ``pipeline_outputs`` — what the Downloads page actually serves. Without
    this step a temporal run leaves the download showing the STALE LEGACY scores
    while the dashboard (which reads the table) shows the temporal ones. Deriving
    the files from the same ``churn_scores`` rows the dashboard reads makes the
    two sources structurally incapable of diverging.

    The export is enriched with the context columns the legacy export carried
    (tier / spend / orders / rating / RFM) via a LEFT JOIN to
    ``mv_customer_features`` and re-uses ``ml.predict.save_scores_csv`` /
    ``save_scores_json``, so the on-disk shape is identical to the legacy one.

    Returns ``(csv_path, json_path)``, or ``(None, None)`` when the tenant has no
    scored rows. ``output_dir`` overrides the default ``ml/output/`` location
    (used by tests); the live pipeline leaves it None.
    """
    from sqlalchemy import text
    from ml.predict import save_scores_csv, save_scores_json

    sql = text(
        """
        SELECT cs.client_id,
               cs.customer_id,
               cs.churn_probability,
               cs.risk_tier               AS risk_level,
               mv.customer_tier,
               mv.total_spend_usd,
               mv.total_orders,
               mv.avg_order_value_usd,
               mv.avg_rating,
               mv.days_since_last_order,
               mv.rfm_total_score,
               cs.driver_1, cs.driver_2, cs.driver_3,
               cs.model_version
          FROM churn_scores cs
          LEFT JOIN mv_customer_features mv
                 ON mv.client_id = cs.client_id
                AND mv.customer_id = cs.customer_id
         WHERE cs.client_id = :cid
         ORDER BY cs.churn_probability DESC
        """
    )
    score_df = pd.read_sql(sql, engine_or_conn, params={"cid": client_id})
    if score_df.empty:
        logger.warning("export_scores_to_disk: no churn_scores for client_id=%s — "
                       "nothing to export", client_id)
        return None, None

    csv_target = None if output_dir is None else Path(output_dir) / "churn_scores.csv"
    json_target = None if output_dir is None else Path(output_dir) / "churn_scores.json"
    csv_path = save_scores_csv(score_df, csv_target)
    json_path = save_scores_json(score_df, json_target)
    logger.info("export_scores_to_disk: wrote %d rows → %s, %s",
                len(score_df), csv_path, json_path)
    return csv_path, json_path
