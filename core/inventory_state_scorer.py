"""
Loads and applies the persisted two-stage inventory-state production model.

Usage:
    from core.inventory_state_scorer import load_production_models, score_dataframe

    models = load_production_models()          # loads from output/models/
    df_scored = score_dataframe(df, models)    # adds pred_move_prob, pred_move_flag, pred_fare_delta
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)

DEFAULT_MODELS_DIR = Path("output/models")


def load_production_models(models_dir: str | Path = DEFAULT_MODELS_DIR) -> dict | None:
    """Load stage_a, stage_b, and metadata from models_dir.

    Returns a dict with keys ``stage_a``, ``stage_b``, ``meta``, or None if the
    model files don't exist yet (training hasn't been run).
    """
    import joblib

    models_path = Path(models_dir)
    stage_a_path = models_path / "stage_a_latest.joblib"
    stage_b_path = models_path / "stage_b_latest.joblib"
    meta_path = models_path / "model_meta_latest.json"

    if not (stage_a_path.exists() and stage_b_path.exists()):
        LOG.warning(
            "Production models not found in '%s'. Run run_training.py (or "
            "train_inventory_state_baseline.py) to train and persist them.",
            models_dir,
        )
        return None

    try:
        stage_a = joblib.load(stage_a_path)
        stage_b = joblib.load(stage_b_path)
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        LOG.info(
            "Loaded production models from '%s' (trained_at=%s rows=%s threshold=%.2f)",
            models_dir,
            meta.get("trained_at_utc", "unknown"),
            meta.get("rows_total", "?"),
            float(meta.get("best_threshold") or 0.5),
        )
        return {"stage_a": stage_a, "stage_b": stage_b, "meta": meta}
    except Exception as exc:
        LOG.error("Failed to load production models from '%s': %s", models_dir, exc)
        return None


def score_dataframe(
    df: pd.DataFrame,
    models: dict | None = None,
    models_dir: str | Path = DEFAULT_MODELS_DIR,
    threshold: float | None = None,
) -> pd.DataFrame | None:
    """Apply the two-stage production model to df.

    Adds three columns to a copy of df:
      - pred_move_prob   : Stage A probability of a price move (0-1)
      - pred_move_flag   : 1 if Stage A predicts a move at the chosen threshold
      - pred_fare_delta  : Predicted fare delta (0 when no move predicted)

    Returns None if models cannot be loaded or scoring fails.
    """
    if models is None:
        models = load_production_models(models_dir)
    if models is None:
        return None

    stage_a = models["stage_a"]
    stage_b = models["stage_b"]
    meta = models.get("meta", {})

    thr = float(threshold if threshold is not None else meta.get("best_threshold") or 0.5)
    feature_cols: list[str] = meta.get("feature_cols") or []

    # Align input to the training feature list; unknown columns are ignored,
    # missing columns are filled with NaN (handled by the pipeline's imputer).
    X = df.reindex(columns=feature_cols)

    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        LOG.debug("Scoring: %d/%d features missing from input (will be imputed)", len(missing), len(feature_cols))

    try:
        p_move = stage_a.predict_proba(X)[:, 1]
        y_pred_move = (p_move >= thr).astype(int)
        delta_pred = stage_b.predict(X)
        pred_delta_combined = np.where(y_pred_move == 1, delta_pred, 0.0)

        out = df.copy()
        out["pred_move_prob"] = p_move
        out["pred_move_flag"] = y_pred_move
        out["pred_fare_delta"] = pred_delta_combined
        return out
    except Exception as exc:
        LOG.error("Scoring failed: %s", exc)
        return None
