"""Train a collections propensity model from the feature mart or application data.

Usage:
  python scripts/train_model.py --db-url postgresql+psycopg2://user:pass@localhost/homecredit --out models/model.joblib
  python scripts/train_model.py --db-path data/cc_data.db --out models/model.joblib
"""
import argparse
import json
import sys
from pathlib import Path

import joblib
import pandas as pd

from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from lightgbm import LGBMClassifier
except Exception:  # pragma: no cover - optional dependency fallback
    LGBMClassifier = None

from scripts.pipeline_utils import (
    build_engine,
    ensure_parent_dir,
    resolve_path,
    relation_names,
)


def choose_relation(engine):
    available = relation_names(engine)
    for candidate in ("mart_model_features", "mart_contact_priority", "mart_recovery_score", "application_train"):
        if candidate in available:
            return candidate
    raise SystemExit("No suitable tables found in DB. Run loader and marts first.")


def load_training_frame(engine, relation_name: str) -> tuple[pd.DataFrame, str, str]:
    frame = pd.read_sql_query(f'SELECT * FROM {relation_name}', engine)
    if relation_name == "application_train":
        if "TARGET" not in frame.columns:
            raise SystemExit('application_train exists but no TARGET column found')
        target_column = "TARGET"
        id_column = "SK_ID_CURR" if "SK_ID_CURR" in frame.columns else frame.columns[0]
        numeric = frame.select_dtypes(include=["int64", "float64", "int32", "float32", "int16", "float16", "bool"]).copy()
        if target_column not in numeric.columns:
            numeric[target_column] = frame[target_column]
        if id_column in numeric.columns:
            numeric = numeric.drop(columns=[id_column])
        return numeric.fillna(0), target_column, id_column

    target_candidates = ["proxy_recovery_label", "recovery_label", "TARGET"]
    target_column = next((column for column in target_candidates if column in frame.columns), None)
    if target_column is None:
        if relation_name == "mart_recovery_score" and "recovery_propensity" in frame.columns:
            frame["proxy_recovery_label"] = (frame["recovery_propensity"] >= frame["recovery_propensity"].median()).astype(int)
            target_column = "proxy_recovery_label"
        else:
            raise SystemExit(f"{relation_name} exists but no target column was found")
    id_column = "SK_ID_CURR" if "SK_ID_CURR" in frame.columns else frame.columns[0]
    numeric = frame.select_dtypes(include=["int64", "float64", "int32", "float32", "int16", "float16", "bool"]).copy()
    excluded_columns = {
        "proxy_recovery_label",
        "collections_scope_flag",
        "recovery_propensity_baseline",
        "priority_score_baseline",
        "expected_recovery_value_proxy",
    }
    numeric = numeric.drop(columns=[column for column in excluded_columns if column in numeric.columns], errors="ignore")
    if target_column not in numeric.columns:
        numeric[target_column] = frame[target_column]
    if id_column in numeric.columns:
        numeric = numeric.drop(columns=[id_column])
    numeric = numeric.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], 0).fillna(0)
    return numeric, target_column, id_column


def top_fraction_recall(y_true, y_score, fraction: float = 0.1) -> float:
    cutoff = max(1, int(len(y_score) * fraction))
    ranked = pd.DataFrame({"y_true": y_true, "y_score": y_score}).sort_values("y_score", ascending=False)
    return float(ranked.head(cutoff)["y_true"].sum() / max(1, ranked["y_true"].sum()))


def top_fraction_lift(y_true, y_score, fraction: float = 0.1) -> float:
    overall_rate = float(pd.Series(y_true).mean())
    if overall_rate == 0:
        return 0.0
    cutoff = max(1, int(len(y_score) * fraction))
    ranked = pd.DataFrame({"y_true": y_true, "y_score": y_score}).sort_values("y_score", ascending=False)
    top_rate = float(ranked.head(cutoff)["y_true"].mean())
    return top_rate / overall_rate


def time_aware_split(frame: pd.DataFrame, target_column: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    time_columns = [
        column
        for column in ("snapshot_month", "snapshot_date", "as_of_date", "as_of_month", "month_index")
        if column in frame.columns
    ]
    if time_columns:
        sort_column = time_columns[0]
        ordered = frame.sort_values(sort_column)
        split_index = max(1, int(len(ordered) * 0.8))
        train_frame = ordered.iloc[:split_index]
        test_frame = ordered.iloc[split_index:]
        if not train_frame.empty and not test_frame.empty and train_frame[target_column].nunique() > 1 and test_frame[target_column].nunique() > 1:
            X_train = train_frame.drop(columns=[target_column])
            y_train = train_frame[target_column]
            X_test = test_frame.drop(columns=[target_column])
            y_test = test_frame[target_column]
            return X_train, X_test, y_train, y_test
    try:
        return train_test_split(
            frame.drop(columns=[target_column]),
            frame[target_column],
            test_size=0.2,
            random_state=42,
            stratify=frame[target_column],
        )
    except ValueError:
        return train_test_split(
            frame.drop(columns=[target_column]),
            frame[target_column],
            test_size=0.2,
            random_state=42,
            stratify=None,
        )


def fit_logistic(X_train, y_train):
    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=2000, class_weight="balanced")),
        ]
    )
    model.fit(X_train, y_train)
    return model


def fit_lightgbm(X_train, y_train):
    if LGBMClassifier is None:
        return None
    model = LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        subsample=0.85,
        colsample_bytree=0.85,
        class_weight="balanced",
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


def predict_scores(model, X_test) -> pd.Series:
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(X_test)
        return pd.Series(probabilities[:, 1], index=X_test.index)
    scores = model.decision_function(X_test)
    return pd.Series(scores, index=X_test.index)


def evaluate_model(y_true, y_score) -> dict[str, float]:
    return {
        "roc_auc": float(roc_auc_score(y_true, y_score)),
        "pr_auc": float(average_precision_score(y_true, y_score)),
        "brier_score": float(brier_score_loss(y_true, y_score)),
        "recall_at_10pct": top_fraction_recall(y_true, y_score, 0.1),
        "lift_at_10pct": top_fraction_lift(y_true, y_score, 0.1),
    }


def main(db_url: str | None, db_path: Path | None, out_path: Path):
    resolved_out_path = resolve_path(out_path)
    engine = build_engine(db_url=db_url, db_path=db_path)
    relation_name = choose_relation(engine)
    frame, target_column, id_column = load_training_frame(engine, relation_name)
    if frame[target_column].nunique() < 2:
        raise SystemExit(f"Target column {target_column} does not contain enough class variation")

    X_train, X_test, y_train, y_test = time_aware_split(frame, target_column)

    candidate_models: dict[str, object] = {
        "logistic_regression": fit_logistic(X_train, y_train),
    }
    lgbm_model = fit_lightgbm(X_train, y_train)
    if lgbm_model is not None:
        candidate_models["lightgbm"] = lgbm_model

    evaluation: dict[str, dict[str, float]] = {}
    champion_name = None
    champion_model = None
    champion_pr_auc = -1.0

    for model_name, model in candidate_models.items():
        y_score = predict_scores(model, X_test)
        metrics = evaluate_model(y_test, y_score)
        evaluation[model_name] = metrics
        if metrics["pr_auc"] > champion_pr_auc:
            champion_pr_auc = metrics["pr_auc"]
            champion_name = model_name
            champion_model = model

    if champion_model is None:
        raise SystemExit("Model training did not produce a valid champion model")

    ensure_parent_dir(resolved_out_path)
    joblib.dump(
        {
            "model": champion_model,
            "relation_name": relation_name,
            "target_column": target_column,
            "id_column": id_column,
            "feature_columns": list(X_train.columns),
            "evaluation": evaluation,
            "champion_model": champion_name,
        },
        resolved_out_path,
    )

    metadata_path = resolved_out_path.with_suffix(".json")
    metadata = {
        "relation_name": relation_name,
        "target_column": target_column,
        "id_column": id_column,
        "feature_columns": list(X_train.columns),
        "evaluation": evaluation,
        "champion_model": champion_name,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Trained model saved to {resolved_out_path}")
    print(json.dumps(metadata, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-url', type=str, default=None)
    parser.add_argument('--db-path', type=Path, default=Path('data/cc_data.db'))
    parser.add_argument('--out', type=Path, default=Path('models/model.joblib'))
    args = parser.parse_args()
    main(args.db_url, args.db_path, args.out)
