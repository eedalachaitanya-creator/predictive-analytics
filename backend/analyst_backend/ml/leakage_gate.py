"""Automated leakage gate (design §9.1–§9.3) — ADDITIVE, the backstop.

The gate PROVES no non-whitelisted feature is a near-deterministic function of
the label. A weak ``corr(feature, label) < 0.95`` gate would miss the recency-as-
proxy failure; the decisive instrument here is a **depth-1 decision stump** — if
a single threshold on one feature reproduces the label at >0.97 accuracy, that
feature is a label encoder and the run hard-fails.

Per feature (measured on the grouped training fold):
  * univariate ROC-AUC (sign-aware)      — suspected leak if > 0.80
  * univariate PR-AUC (average precision) — reported
  * depth-1 stump accuracy                — BAN if > 0.97 (the decisive test)
  * |spearman(f, y)|                      — gate <= 0.90
  * normalized mutual information         — gate <= 0.60
  * train-vs-test PSI                     — FLAG if > 0.25 (drift diagnostic)

Hard fails (raise ``LeakageGateError`` naming the feature) if:
  * any non-whitelisted feature trips the stump (> 0.97), OR
  * any §6.4-excluded family name appears in ``X``.

The returned dict is persisted verbatim in ``metadata.leakage_gate``.
"""
from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional, Sequence

import numpy as np
import pandas as pd

logger = logging.getLogger("ml.leakage_gate")

# Thresholds (design §9.1).
AUC_GATE = 0.80
STUMP_GATE = 0.97
SPEARMAN_GATE = 0.90
MUTUAL_INFO_GATE = 0.60
PSI_FLAG = 0.25

# §6.4 UNCONDITIONALLY-excluded families — any of these names in X is a hard fail
# regardless of statistics, because they are NOT point-in-time reconstructable:
#   * login family       — `customers.last_login_date` is a single mutable
#                          "last login ever" column with no event log; its as-of-T
#                          value is unrecoverable (§6.4 / red-team H5).
#   * subscription/refill — sourced from the whole-history `vw_subscription_products`
#                          view; dropped this iteration (§6.4 / red-team H6).
#   * old MV label inputs — replaced by the forward label; never features.
#
# NOTE (deliberately NOT here): the ticket-resolution metrics
# (`avg_resolution_time_hrs`, `pct_tickets_resolved`, `open_tickets`,
# `critical_tickets`, …). Per spec §6.4 these are RECONSTRUCTABLE once the builder
# gates `resolved_date <= T` and recomputes the open-duration as-of-T — which the
# point-in-time builder does. They are therefore admitted as features and judged
# by the per-feature stump/AUC tests below like any other feature, NOT hard-failed
# by name. (A genuine post-T resolution leak would still be caught by the stump.)
EXCLUDED_FEATURE_NAMES = frozenset({
    "last_login_date", "days_since_last_login",
    "avg_refill_cycle_days", "subscription_product_count",
    "missed_refill_count", "days_overdue_for_refill",
    "churn_label", "churn_window_days", "login_window_days",
})


class LeakageGateError(Exception):
    """Raised when the leakage gate hard-fails — the run must not persist a
    promotable bundle (design §9.3)."""


def _clean_pair(f: np.ndarray, y: np.ndarray):
    """Drop NaN/inf rows from a (feature, label) pair for a univariate test."""
    f = np.asarray(f, dtype=float)
    y = np.asarray(y, dtype=int)
    ok = np.isfinite(f)
    return f[ok], y[ok]


def _univariate_auc(f: np.ndarray, y: np.ndarray) -> float:
    """Sign-aware univariate ROC-AUC: a feature anti-correlated with the label is
    just as leaky as one positively correlated, so report max(auc, 1-auc)."""
    from sklearn.metrics import roc_auc_score

    f, y = _clean_pair(f, y)
    if len(np.unique(y)) < 2 or len(f) < 4:
        return 0.5
    auc = roc_auc_score(y, f)
    return float(max(auc, 1.0 - auc))


def _univariate_pr_auc(f: np.ndarray, y: np.ndarray) -> float:
    from sklearn.metrics import average_precision_score

    f, y = _clean_pair(f, y)
    if len(np.unique(y)) < 2 or len(f) < 4:
        return float(np.mean(y)) if len(y) else 0.0
    # orient the score toward the positive class (sign-aware, like AUC).
    ap_pos = average_precision_score(y, f)
    ap_neg = average_precision_score(y, -f)
    return float(max(ap_pos, ap_neg))


def _stump_accuracy(f: np.ndarray, y: np.ndarray) -> float:
    """Depth-1 decision stump accuracy — the decisive near-determinism test."""
    from sklearn.tree import DecisionTreeClassifier

    f, y = _clean_pair(f, y)
    if len(np.unique(y)) < 2 or len(f) < 4:
        return float(max(np.mean(y == 0), np.mean(y == 1))) if len(y) else 0.0
    stump = DecisionTreeClassifier(max_depth=1, random_state=0)
    stump.fit(f.reshape(-1, 1), y)
    return float(stump.score(f.reshape(-1, 1), y))


def _abs_spearman(f: np.ndarray, y: np.ndarray) -> float:
    from scipy.stats import spearmanr

    f, y = _clean_pair(f, y)
    if len(f) < 4 or np.unique(f).size < 2 or np.unique(y).size < 2:
        return 0.0
    rho, _ = spearmanr(f, y)
    return float(abs(rho)) if np.isfinite(rho) else 0.0


def _norm_mutual_info(f: np.ndarray, y: np.ndarray) -> float:
    """mutual_info_classif(f; y) normalized by the label entropy → [0, 1]."""
    from sklearn.feature_selection import mutual_info_classif

    f, y = _clean_pair(f, y)
    if len(f) < 4 or np.unique(y).size < 2:
        return 0.0
    mi = float(mutual_info_classif(
        f.reshape(-1, 1), y, discrete_features=False, random_state=0)[0])
    p = float(np.mean(y))
    p = min(max(p, 1e-12), 1 - 1e-12)
    h_y = -(p * np.log(p) + (1 - p) * np.log(1 - p))  # nats
    if h_y <= 0:
        return 0.0
    return float(min(mi / h_y, 1.0))


def _psi(train: np.ndarray, test: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index between two distributions of one feature."""
    train = np.asarray(train, dtype=float)
    test = np.asarray(test, dtype=float)
    train = train[np.isfinite(train)]
    test = test[np.isfinite(test)]
    if len(train) < 2 or len(test) < 2 or np.unique(train).size < 2:
        return 0.0
    qs = np.quantile(train, np.linspace(0, 1, bins + 1))
    edges = np.unique(qs)
    if edges.size < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    tr_hist, _ = np.histogram(train, bins=edges)
    te_hist, _ = np.histogram(test, bins=edges)
    tr_pct = np.clip(tr_hist / max(tr_hist.sum(), 1), 1e-6, None)
    te_pct = np.clip(te_hist / max(te_hist.sum(), 1), 1e-6, None)
    return float(np.sum((te_pct - tr_pct) * np.log(te_pct / tr_pct)))


def run_leakage_gate(
    X: pd.DataFrame,
    y: Sequence[int],
    groups: Sequence,
    *,
    X_test: Optional[pd.DataFrame] = None,
    y_test: Optional[Sequence[int]] = None,
    whitelist: Iterable[str] = (),
) -> Dict:
    """Run the per-feature leakage gate on the grouped TRAIN fold (design §9).

    ``X``/``y`` are the grouped training fold; ``groups`` are the customer ids
    (used to compute the train-vs-test PSI by a group-disjoint half-split when an
    explicit ``X_test`` is not supplied). Returns a dict with ``passed``,
    ``per_feature``, ``whitelist``, ``thresholds`` for ``metadata.leakage_gate``.
    Raises ``LeakageGateError`` (naming the feature) on a hard fail.
    """
    whitelist = set(whitelist)
    y = np.asarray(list(y), dtype=int)
    groups = np.asarray(list(groups))

    # ── Hard fail #1: any §6.4-excluded family name present in X. ─────────────
    present_excluded = sorted(EXCLUDED_FEATURE_NAMES & set(X.columns))
    if present_excluded:
        raise LeakageGateError(
            "§6.4-excluded feature(s) present in X (must never be a feature): "
            f"{present_excluded}"
        )

    # PSI reference split: prefer an explicit test frame; otherwise split THIS
    # fold into two group-disjoint halves so PSI is a real drift diagnostic and
    # never trivially zero.
    if X_test is not None:
        psi_ref = {c: np.asarray(X_test[c], dtype=float) for c in X.columns if c in X_test.columns}
        psi_cur = {c: np.asarray(X[c], dtype=float) for c in X.columns}
    else:
        uniq = np.array(sorted(set(groups.tolist())))
        half = uniq[: max(1, len(uniq) // 2)]
        mask_a = np.isin(groups, half)
        psi_ref = {c: np.asarray(X[c], dtype=float)[mask_a] for c in X.columns}
        psi_cur = {c: np.asarray(X[c], dtype=float)[~mask_a] for c in X.columns}

    per_feature: Dict[str, Dict[str, float]] = {}
    flagged_psi: list = []
    soft_violations: list = []
    stump_bans: list = []

    for col in X.columns:
        f = np.asarray(X[col], dtype=float)
        auc = _univariate_auc(f, y)
        pr = _univariate_pr_auc(f, y)
        stump = _stump_accuracy(f, y)
        sp = _abs_spearman(f, y)
        mi = _norm_mutual_info(f, y)
        psi = _psi(psi_ref.get(col, f), psi_cur.get(col, f))

        per_feature[col] = {
            "univariate_auc": round(auc, 6),
            "univariate_pr_auc": round(pr, 6),
            "stump_acc": round(stump, 6),
            "abs_spearman": round(sp, 6),
            "norm_mutual_info": round(mi, 6),
            "psi": round(psi, 6),
            "whitelisted": col in whitelist,
        }

        if col in whitelist:
            continue  # §9.2 — the single best legitimate signal is not auto-killed

        if stump > STUMP_GATE:
            stump_bans.append((col, stump))
        if auc > AUC_GATE:
            soft_violations.append((col, "univariate_auc", auc))
        if sp > SPEARMAN_GATE:
            soft_violations.append((col, "abs_spearman", sp))
        if mi > MUTUAL_INFO_GATE:
            soft_violations.append((col, "norm_mutual_info", mi))
        if psi > PSI_FLAG:
            flagged_psi.append((col, psi))

    # ── Hard fail #2: any non-whitelisted feature trips the stump (§9.3). ──────
    if stump_bans:
        names = ", ".join(f"{c} (stump_acc={v:.4f})" for c, v in stump_bans)
        raise LeakageGateError(
            f"near-deterministic label encoder(s) banned by depth-1 stump (>{STUMP_GATE}): {names}"
        )

    result = {
        "passed": True,
        "per_feature": per_feature,
        "whitelist": sorted(whitelist),
        "soft_violations": [
            {"feature": c, "metric": m, "value": round(float(v), 6)}
            for c, m, v in soft_violations
        ],
        "psi_flags": [{"feature": c, "psi": round(float(v), 6)} for c, v in flagged_psi],
        "thresholds": {
            "univariate_auc": AUC_GATE,
            "stump_acc": STUMP_GATE,
            "abs_spearman": SPEARMAN_GATE,
            "norm_mutual_info": MUTUAL_INFO_GATE,
            "psi_flag": PSI_FLAG,
        },
    }
    if soft_violations:
        logger.warning(
            "leakage_gate: %d soft AUC/spearman/MI violation(s) (no stump ban) — "
            "review %s", len(soft_violations),
            sorted({c for c, _, _ in soft_violations}),
        )
    if flagged_psi:
        logger.warning("leakage_gate: PSI drift flagged for %s",
                       [c for c, _ in flagged_psi])
    logger.info("leakage_gate: PASSED on %d features", len(per_feature))
    return result
