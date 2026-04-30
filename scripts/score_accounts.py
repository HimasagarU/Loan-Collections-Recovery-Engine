"""Score collection accounts with a trained propensity model."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pipeline_utils import build_engine, ensure_parent_dir, relation_exists, resolve_path


def load_model_payload(model_path: Path) -> dict:
    payload = joblib.load(model_path)
    if isinstance(payload, dict) and "model" in payload:
        return payload
    return {"model": payload, "feature_columns": None, "relation_name": None, "target_column": None}


def predict_scores(model, frame: pd.DataFrame) -> pd.Series:
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(frame)
        return pd.Series(probabilities[:, 1], index=frame.index)
    if hasattr(model, "decision_function"):
        scores = model.decision_function(frame)
        return pd.Series(scores, index=frame.index)
    predictions = model.predict(frame)
    return pd.Series(predictions, index=frame.index)


def build_scored_frame(feature_frame: pd.DataFrame, payload: dict, model_name: str) -> pd.DataFrame:
    feature_columns = payload.get("feature_columns")
    if feature_columns:
        scoring_frame = feature_frame.reindex(columns=feature_columns, fill_value=0).copy()
    else:
        scoring_frame = feature_frame.select_dtypes(include=["int64", "float64", "int32", "float32", "int16", "float16", "bool"]).copy()

    scoring_frame = scoring_frame.apply(pd.to_numeric, errors="coerce").fillna(0)
    model = payload["model"]
    model_score = predict_scores(model, scoring_frame)

    scored = feature_frame.copy()
    scored["model_score"] = model_score
    scored["expected_recovery_value"] = scored["model_score"] * scored.get("expected_recovery_value_proxy", scored.get("outstanding_balance_proxy", 0))
    scored["priority_score"] = scored["expected_recovery_value"] * (1 + scored.get("installment_max_delay_days", 0).fillna(0) / 90.0) * (1 + scored.get("monthly_recent_3_stress_rate", 0).fillna(0))
    scored = scored.sort_values(["priority_score", "expected_recovery_value", "model_score"], ascending=[False, False, False]).reset_index(drop=True)
    scored["priority_rank"] = scored.index + 1
    scored["priority_decile"] = ((scored["priority_rank"] - 1) * 10 // max(len(scored), 1)) + 1
    scored["model_name"] = model_name
    scored["scored_at_utc"] = datetime.now(timezone.utc).isoformat()
    return scored


def main(db_url: str | None, db_path: Path | None, model_path: Path, feature_relation: str, output_relation: str):
    engine = build_engine(db_url=db_url, db_path=db_path)
    resolved_model_path = resolve_path(model_path)
    if not resolved_model_path.exists():
        raise SystemExit(f"Model file not found: {resolved_model_path}")
    if not relation_exists(engine, feature_relation):
        raise SystemExit(f"Feature relation not found: {feature_relation}")

    payload = load_model_payload(resolved_model_path)
    model_name = payload.get("champion_model", resolved_model_path.stem)
    feature_frame = pd.read_sql_query(f"SELECT * FROM {feature_relation}", engine)
    scored_frame = build_scored_frame(feature_frame, payload, model_name)

    ensure_parent_dir(resolve_path(output_relation if output_relation.endswith(".csv") else Path("models") / f"{output_relation}.tmp"))
    scored_frame.to_sql(output_relation, engine, if_exists="replace", index=False, chunksize=5000, method="multi")

    metadata = {
        "model_path": str(resolved_model_path),
        "feature_relation": feature_relation,
        "output_relation": output_relation,
        "rows_scored": int(len(scored_frame)),
        "model_name": model_name,
    }
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", type=str, default=None)
    parser.add_argument("--db-path", type=Path, default=Path("data/cc_data.db"))
    parser.add_argument("--model-path", type=Path, default=Path("models/model.joblib"))
    parser.add_argument("--feature-relation", type=str, default="mart_model_features")
    parser.add_argument("--output-relation", type=str, default="mart_scored_accounts")
    args = parser.parse_args()
    main(args.db_url, args.db_path, args.model_path, args.feature_relation, args.output_relation)