from __future__ import annotations
import argparse
import os

import numpy as np
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from ml.train_temporal import build_feature_matrix, read_snapshots

EMOTION_FEATURE_COLS = (
    "mean_ticket_distress", "max_ticket_distress", "pct_negative_emotion_tickets",
    "negative_tickets_30d", "had_disappointed_ticket_30d", "days_since_worst_ticket",
    "mean_review_distress", "max_review_distress", "pct_negative_emotion_reviews",
)


def _fit_pr_auc(X, y, Xte, yte, seed):
    pos = max(1, int((y == 1).sum()))
    neg = max(1, int((y == 0).sum()))
    clf = XGBClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=neg / pos, eval_metric="aucpr",
        random_state=seed, n_jobs=2,
    )
    clf.fit(X.fillna(0), y)
    proba = clf.predict_proba(Xte.fillna(0))[:, 1]
    return float(average_precision_score(yte, proba))


def _train_eval(df, test_frac: float = 0.2, seed: int = 42) -> dict:
    Xf, y, _ = build_feature_matrix(df)
    Xb, _, _ = build_feature_matrix(df, exclude_cols=EMOTION_FEATURE_COLS)
    idx = np.arange(len(df))
    tr, te = train_test_split(idx, test_size=test_frac, random_state=seed,
                              stratify=y if y.nunique() > 1 else None)
    full = _fit_pr_auc(Xf.iloc[tr], y.iloc[tr], Xf.iloc[te], y.iloc[te], seed)
    base = _fit_pr_auc(Xb.iloc[tr], y.iloc[tr], Xb.iloc[te], y.iloc[te], seed)
    return {"baseline_pr_auc": round(base, 4), "full_pr_auc": round(full, 4),
            "delta": round(full - base, 4), "n_rows": int(len(df))}


def run_experiment(engine, client_id: str, test_frac: float = 0.2,
                   seed: int = 42) -> dict:
    df = read_snapshots(engine, client_id)
    if df.empty:
        return {"error": "no snapshots; run the temporal pipeline first"}
    return _train_eval(df, test_frac=test_frac, seed=seed)


def main():
    ap = argparse.ArgumentParser(description="Emotion-feature PR-AUC lift")
    ap.add_argument("--db-url", default=os.getenv("DB_URL"))
    ap.add_argument("--client-id", required=True)
    a = ap.parse_args()
    if not a.db_url:
        ap.error("--db-url is required (or set DB_URL)")
    from sqlalchemy import create_engine
    eng = create_engine(a.db_url, future=True)
    res = run_experiment(eng, a.client_id)
    print(f"baseline PR-AUC: {res.get('baseline_pr_auc')}  "
          f"full PR-AUC: {res.get('full_pr_auc')}  delta: {res.get('delta')}")


if __name__ == "__main__":
    main()
