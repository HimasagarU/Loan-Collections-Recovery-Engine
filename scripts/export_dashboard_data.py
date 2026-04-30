"""Export dashboard-ready views and tables to CSV files for Power BI."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pipeline_utils import DEFAULT_EXPORTS_DIR, build_engine, ensure_parent_dir, relation_exists, resolve_path


DEFAULT_RELATIONS = (
    "mart_scored_accounts",
    "mart_contact_priority",
    "mart_delinquency_aging",
    "vw_kpi_summary",
    "vw_recovery_cohort_summary",
    "vw_channel_summary",
    "vw_data_quality_summary",
    "dim_risk_band",
    "dim_channel",
)


def export_relation(engine, relation_name: str, export_dir: Path) -> Path | None:
    if not relation_exists(engine, relation_name):
        print(f"Skipping missing relation: {relation_name}")
        return None

    export_path = export_dir / f"{relation_name}.csv"
    frame = pd.read_sql_query(f"SELECT * FROM {relation_name}", engine)
    frame.to_csv(export_path, index=False, encoding="utf-8-sig")
    return export_path


def main(db_url: str | None, db_path: Path | None, export_dir: Path, relations: list[str]):
    engine = build_engine(db_url=db_url, db_path=db_path)
    resolved_export_dir = resolve_path(export_dir)
    ensure_parent_dir(resolved_export_dir / "placeholder.csv")
    resolved_export_dir.mkdir(parents=True, exist_ok=True)

    exported_paths: list[Path] = []
    for relation_name in relations:
        exported_path = export_relation(engine, relation_name, resolved_export_dir)
        if exported_path is not None:
            exported_paths.append(exported_path)

    print("Exported files:")
    for path in exported_paths:
        print(f"- {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", type=str, default=None)
    parser.add_argument("--db-path", type=Path, default=Path("data/cc_data.db"))
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_EXPORTS_DIR)
    parser.add_argument("--relations", type=str, nargs="+", default=list(DEFAULT_RELATIONS))
    args = parser.parse_args()
    main(args.db_url, args.db_path, args.export_dir, args.relations)