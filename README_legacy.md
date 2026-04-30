# Collections Prioritization & Recovery Propensity Engine

SQL-first analytics + ML scaffold for the Home Credit Default Risk dataset.

Quick start

1. Download the Home Credit dataset from Kaggle and place CSVs into `data/`.
2. Install dependencies: `pip install -r requirements.txt`.
3. Load CSVs into a local SQLite DB:
   `python scripts/load_csvs_to_sqlite.py --data-dir data --db-path data/cc_data.db`
4. Build marts (runs SQL in `sql/marts.sql`):
   `python scripts/build_marts.py --db-path data/cc_data.db --sql sql/marts.sql`
5. Run model training (basic example):
   `python scripts/train_model.py --db-path data/cc_data.db --out models/model.joblib`