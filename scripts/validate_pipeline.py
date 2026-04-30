"""Validate the collections analytics pipeline end to end."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pipeline_utils import build_engine, relation_exists, resolve_path


REQUIRED_RELATIONS = (
    "application_train",
    "installments_payments",
    "fact_installment",
    "mart_model_features",
    "mart_contact_priority",
    "vw_kpi_summary",
    "vw_data_quality_summary",
)


def main(db_url: str | None, db_path: Path | None, model_path: Path | None, require_scored_table: bool):
    engine = build_engine(db_url=db_url, db_path=db_path)

    missing_relations = [relation for relation in REQUIRED_RELATIONS if not relation_exists(engine, relation)]
    if missing_relations:
        raise SystemExit(f"Missing required relations: {', '.join(missing_relations)}")

    quality_frame = pd.read_sql_query("SELECT * FROM vw_data_quality_summary", engine)
    failed_checks = quality_frame[quality_frame["status"] == "fail"]
    if not failed_checks.empty:
        print("Data quality failures detected:")
        print(failed_checks.to_string(index=False))
        raise SystemExit(1)

    mart_features_count = int(pd.read_sql_query("SELECT COUNT(*) AS row_count FROM mart_model_features", engine).iloc[0]["row_count"])
    if mart_features_count == 0:
        raise SystemExit("mart_model_features is empty")

    contact_queue_count = int(pd.read_sql_query("SELECT COUNT(*) AS row_count FROM mart_contact_priority", engine).iloc[0]["row_count"])
    if contact_queue_count == 0:
        raise SystemExit("mart_contact_priority is empty")

    scored_count = None
    if require_scored_table:
        if not relation_exists(engine, "mart_scored_accounts"):
            raise SystemExit("mart_scored_accounts is missing")
        scored_count = int(pd.read_sql_query("SELECT COUNT(*) AS row_count FROM mart_scored_accounts", engine).iloc[0]["row_count"])
        if scored_count == 0:
            raise SystemExit("mart_scored_accounts is empty")

    payload_info = {}
    if model_path is not None:
        resolved_model_path = resolve_path(model_path)
        if not resolved_model_path.exists():
            raise SystemExit(f"Model file not found: {resolved_model_path}")
        payload = joblib.load(resolved_model_path)
        payload_info = {
            "model_path": str(resolved_model_path),
            "payload_type": type(payload).__name__,
            "has_model_key": bool(isinstance(payload, dict) and "model" in payload),
        }

    report = {
        "mart_model_features_rows": mart_features_count,
        "mart_contact_priority_rows": contact_queue_count,
        "mart_scored_accounts_rows": scored_count,
        "quality_check_rows": int(len(quality_frame)),
        "payload_info": payload_info,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", type=str, default=None)
    parser.add_argument("--db-path", type=Path, default=Path("data/cc_data.db"))
    parser.add_argument("--model-path", type=Path, default=Path("models/model.joblib"))
    parser.add_argument("--require-scored-table", action="store_true")
    args = parser.parse_args()
    main(args.db_url, args.db_path, args.model_path, args.require_scored_table)