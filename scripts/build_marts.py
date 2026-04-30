"""Run SQL files against the database to create marts/views/tables.

Usage:
  python scripts/build_marts.py --db-url postgresql+psycopg2://user:pass@localhost/homecredit --sql sql/schema.sql sql/marts.sql
  python scripts/build_marts.py --db-path data/cc_data.db --sql sql/marts.sql
"""
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pipeline_utils import build_engine, resolve_path, run_sql_files


def run_sql_files_on_database(db_url: str | None, db_path: Path | None, sql_paths: list[Path]):
    engine = build_engine(db_url=db_url, db_path=db_path)
    resolved_sql_paths = [resolve_path(path) for path in sql_paths]
    run_sql_files(engine, resolved_sql_paths)
    print('SQL execution complete.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-url', type=str, default=None)
    parser.add_argument('--db-path', type=Path, default=Path('data/cc_data.db'))
    parser.add_argument('--sql', type=Path, nargs='+', default=[Path('sql/schema.sql'), Path('sql/marts.sql'), Path('sql/quality_checks.sql')])
    args = parser.parse_args()
    if args.db_url is None and not resolve_path(args.db_path).exists():
        print(f"Database not found: {resolve_path(args.db_path)}")
    run_sql_files_on_database(args.db_url, args.db_path, args.sql)
