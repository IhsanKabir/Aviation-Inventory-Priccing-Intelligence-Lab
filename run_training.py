"""
Training pipeline runner.

Chains two steps:
  1. build_inventory_state_dataset.py  — queries DB, writes inventory_state_v2_latest.csv
  2. train_inventory_state_baseline.py — trains two-stage model, saves production .joblib files

Called by the scheduler for training_enrichment (daily) and training_deep (Sunday) windows,
or run manually:

    python run_training.py
    python run_training.py --skip-dataset-build   # reuse existing CSV
    python run_training.py --stage-b-model rf
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent

DEFAULT_MODELS_DIR = "output/models"
DEFAULT_OUTPUT_DIR = "output/reports"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_SCHEMA_VERSION = "inventory_state_v2"


def _run_step(label: str, cmd: list[str]) -> int:
    print(f"[run_training] {label}: {' '.join(str(c) for c in cmd[:5])}...")
    rc = subprocess.call(cmd)
    if rc != 0:
        print(f"[run_training] {label} FAILED (rc={rc})", file=sys.stderr)
    return rc


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build dataset + train two-stage inventory-state production model"
    )
    p.add_argument("--db-url", help="Override database URL (default: from env / config)")
    p.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Days of history to include in the dataset build",
    )
    p.add_argument(
        "--schema-version",
        choices=["inventory_state_v1", "inventory_state_v2"],
        default=DEFAULT_SCHEMA_VERSION,
    )
    p.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--airline", help="Limit training to this airline code")
    p.add_argument(
        "--stage-b-model",
        choices=["ridge", "rf"],
        default="ridge",
        help="Stage B regressor architecture for the two-stage model",
    )
    p.add_argument("--stage-a-calibration", choices=["none", "sigmoid", "isotonic"], default="none")
    p.add_argument("--min-move-delta", type=float, default=0.0)
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument(
        "--skip-dataset-build",
        action="store_true",
        help="Reuse existing CSV from --output-dir; skip the DB query step",
    )
    p.add_argument(
        "--no-save-models",
        action="store_true",
        help="Evaluate only; do not write .joblib artifacts",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    py = args.python_exe
    out_dir = args.output_dir

    print(f"[run_training] started at {datetime.now(timezone.utc).isoformat()}")

    # Step 1: Build dataset CSV from DB
    if not args.skip_dataset_build:
        build_cmd = [
            py,
            str(REPO_ROOT / "tools" / "build_inventory_state_dataset.py"),
            "--schema-version", args.schema_version,
            "--output-dir", out_dir,
            "--lookback-days", str(args.lookback_days),
            "--format", "csv",
        ]
        if args.db_url:
            build_cmd += ["--db-url", args.db_url]
        if _run_step("build_dataset", build_cmd) != 0:
            return 1
    else:
        print("[run_training] build_dataset: skipped (--skip-dataset-build)")

    # Step 2: Train and persist production models
    train_cmd = [
        py,
        str(REPO_ROOT / "tools" / "train_inventory_state_baseline.py"),
        "--output-dir", out_dir,
        "--models-dir", args.models_dir,
        "--stage-b-model", args.stage_b_model,
        "--stage-a-calibration", args.stage_a_calibration,
        "--min-move-delta", str(args.min_move_delta),
    ]
    if args.airline:
        train_cmd += ["--airline", args.airline]
    if args.no_save_models:
        train_cmd.append("--no-save-models")
    if _run_step("train", train_cmd) != 0:
        return 1

    print(f"[run_training] done at {datetime.now(timezone.utc).isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
