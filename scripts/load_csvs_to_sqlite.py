"""Load CSV files from a directory into a database as staging tables.

Usage:
  python scripts/load_csvs_to_sqlite.py --data-dir data --db-url postgresql+psycopg2://user:pass@localhost/homecredit
  python scripts/load_csvs_to_sqlite.py --data-dir data --db-path data/cc_data.db
"""
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pipeline_utils import (
    build_engine,
    discover_csv_load_plan,
    ensure_parent_dir,
    load_csv_to_table,
    resolve_path,
)


def main(data_dir: Path, db_url: str | None, db_path: Path | None):
    resolved_data_dir = resolve_path(data_dir)
    engine = build_engine(db_url=db_url, db_path=db_path)
    csv_plan = discover_csv_load_plan(resolved_data_dir)
    if not csv_plan:
        print(f"No CSV files found in {resolved_data_dir}")
        return

    for csv_path, table_name in csv_plan:
        print(f"Loading {csv_path.name} -> {table_name}")
        row_count = load_csv_to_table(engine, csv_path, table_name)
        print(f"Wrote {row_count:,} rows to {table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=Path, default=Path('data'))
    parser.add_argument('--db-url', type=str, default=None)
    parser.add_argument('--db-path', type=Path, default=Path('data/cc_data.db'))
    args = parser.parse_args()
    resolved_data_dir = resolve_path(args.data_dir)
    resolved_data_dir.mkdir(parents=True, exist_ok=True)
    if args.db_url is None and args.db_path is not None:
        ensure_parent_dir(resolve_path(args.db_path))
    main(resolved_data_dir, args.db_url, args.db_path)
