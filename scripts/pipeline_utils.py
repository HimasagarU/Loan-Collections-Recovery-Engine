"""Shared helpers for the collections analytics pipeline."""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Sequence

import pandas as pd
from sqlalchemy import BigInteger, Boolean, DateTime, Float, Text, create_engine, inspect
from sqlalchemy.engine import Engine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_SQL_DIR = PROJECT_ROOT / "sql"
DEFAULT_MODELS_DIR = PROJECT_ROOT / "models"
DEFAULT_EXPORTS_DIR = PROJECT_ROOT / "exports"

DEFAULT_LOAD_ORDER = (
    "application_train",
    "application_test",
    "bureau",
    "bureau_balance",
    "previous_application",
    "pos_cash_balance",
    "installments_payments",
    "credit_card_balance",
    "homecredit_columns_description",
)


def ensure_project_root_on_path() -> Path:
    """Make project imports work when a script is launched directly from cmd."""
    root = PROJECT_ROOT
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def resolve_path(value: str | Path, base: Path | None = None) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base or PROJECT_ROOT) / path


def normalize_name(value: str) -> str:
    value = value.strip().lower().replace("-", "_")
    value = re.sub(r"[^0-9a-zA-Z_]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def build_engine(db_url: str | None = None, db_path: str | Path | None = None) -> Engine:
    if db_url:
        return create_engine(db_url)
    resolved_path = resolve_path(db_path or (DEFAULT_DATA_DIR / "cc_data.db"))
    return create_engine(f"sqlite:///{resolved_path.as_posix()}")


def discover_csv_load_plan(data_dir: Path) -> list[tuple[Path, str]]:
    csv_files = [path for path in data_dir.glob("*.csv") if path.is_file()]
    rank = {name: index for index, name in enumerate(DEFAULT_LOAD_ORDER)}

    def sort_key(path: Path) -> tuple[int, str]:
        table_name = normalize_name(path.stem)
        return (rank.get(table_name, len(rank)), table_name)

    return [(path, normalize_name(path.stem)) for path in sorted(csv_files, key=sort_key)]


def infer_sqlalchemy_dtype_map(df: pd.DataFrame) -> dict[str, object]:
    dtype_map: dict[str, object] = {}
    for column_name, series in df.items():
        if pd.api.types.is_bool_dtype(series):
            dtype_map[column_name] = Boolean()
        elif pd.api.types.is_integer_dtype(series):
            dtype_map[column_name] = BigInteger()
        elif pd.api.types.is_float_dtype(series):
            dtype_map[column_name] = Float()
        elif pd.api.types.is_datetime64_any_dtype(series):
            dtype_map[column_name] = DateTime()
        else:
            dtype_map[column_name] = Text()
    return dtype_map


def load_csv_to_table(engine: Engine, csv_path: Path, table_name: str) -> int:
    frame = pd.read_csv(csv_path)
    normalized_table_name = normalize_name(table_name)
    frame.to_sql(
        normalized_table_name,
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
        dtype=infer_sqlalchemy_dtype_map(frame),
    )
    return len(frame)


def relation_names(engine: Engine) -> set[str]:
    inspector = inspect(engine)
    return set(inspector.get_table_names()) | set(inspector.get_view_names())


def relation_exists(engine: Engine, name: str) -> bool:
    return normalize_name(name) in relation_names(engine)


def split_sql_script(sql_text: str) -> list[str]:
    cleaned_lines: list[str] = []
    for raw_line in sql_text.splitlines():
        line = re.sub(r"--.*$", "", raw_line).strip()
        if line:
            cleaned_lines.append(line)
    merged = " ".join(cleaned_lines)
    return [statement.strip() for statement in merged.split(";") if statement.strip()]


def run_sql_files(engine: Engine, sql_files: Sequence[Path]) -> None:
    with engine.begin() as connection:
        for sql_file in sql_files:
            resolved = resolve_path(sql_file)
            if not resolved.exists():
                raise FileNotFoundError(f"SQL file not found: {resolved}")
            statements = split_sql_script(resolved.read_text(encoding="utf-8"))
            for statement in statements:
                connection.exec_driver_sql(statement)


def ensure_parent_dir(path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)