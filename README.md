# Loan Collections Recovery Engine

PostgreSQL, SQL, and Python project for prioritizing overdue loan accounts and estimating recovery likelihood.

This repository turns the Home Credit Default Risk dataset into a collections-style warehouse and scoring pipeline. The goal is to identify overdue accounts, estimate repayment likelihood, rank cases by expected value, and generate analytical outputs from SQL and Python.

## Project Overview

The project answers a simple collections question: which overdue accounts should be contacted first to maximize expected recovery?

To support that, the pipeline includes:

- SQL warehouse design
- delinquency and bureau feature engineering
- account prioritization logic
- supervised learning for recovery propensity
- dashboard-ready exports

## Dataset

Source dataset: Home Credit Default Risk from Kaggle.

The dataset is useful because it contains several related tables instead of a single flat file. That makes it possible to demonstrate warehouse design, joins, window functions, and model-ready feature building.

Main tables used:

- `application_train` and `application_test` for borrower and loan application data
- `bureau` and `bureau_balance` for bureau history and stress signals
- `previous_application` for prior lending behavior
- `installments_payments` for repayment timing and delinquency patterns
- `pos_cash_balance` and `credit_card_balance` for monthly account behavior

The project uses a proxy recovery label because the dataset does not include a native collections outcome. The label is derived from delinquency severity and cure behavior.

## Technical Approach

The warehouse layer is built with PostgreSQL-compatible SQL and includes:

- staging tables for raw loads
- dimension tables for customers, loans, time, risk bands, and channels
- fact tables for installment history, bureau snapshots, prior applications, and monthly account state
- analytical marts for delinquency aging, recovery score, and contact prioritization
- data-quality views for row counts, duplicates, null keys, and orphan keys

The modeling layer uses:

- logistic regression as an interpretable baseline
- LightGBM as a stronger nonlinear model when available
- ROC-AUC, PR-AUC, Brier score, recall at top 10%, and lift at top 10% for evaluation

## Skills And Concepts Applied

- SQL joins, CTEs, subqueries, and window functions
- warehouse-style staging, fact, and dimension design
- time-aware snapshot modeling
- feature engineering for delinquency, bureau stress, and payment behavior
- data validation and referential-integrity checks
- model ranking and account prioritization
- PostgreSQL connectivity through SQLAlchemy
- pandas, scikit-learn, LightGBM, and joblib

## Main Outputs

- `mart_model_features`: feature table for modeling
- `mart_contact_priority`: ranked collection queue
- `mart_delinquency_aging`: aging summary table
- `mart_scored_accounts`: model-scored accounts
- `vw_kpi_summary`: business KPI view
- CSV exports in `exports/` for Power BI

## Setup

1. Open Windows cmd in the repository root.
2. Create the conda environment:
   `conda env create -f environment.yml`
3. Activate it:
   `conda activate creditcard-collections`
4. Create your PostgreSQL user and database, then set the connection string:
   `set CC_DATABASE_URL=postgresql+psycopg2://credit_user:YourStrongPassword@localhost:5432/homecredit`
5. Download the Kaggle Home Credit CSVs and place them in `data\`.

## Run The Pipeline

1. Load the raw CSVs into staging:
   `python scripts\load_csvs_to_sqlite.py --data-dir data --db-url %CC_DATABASE_URL%`
2. Build the warehouse objects and quality views:
   `python scripts\build_marts.py --db-url %CC_DATABASE_URL%`
3. Train the propensity model:
   `python scripts\train_model.py --db-url %CC_DATABASE_URL% --out models\model.joblib`
4. Score the account queue:
   `python scripts\score_accounts.py --db-url %CC_DATABASE_URL% --model-path models\model.joblib`
5. Validate the warehouse and outputs:
   `python scripts\validate_pipeline.py --db-url %CC_DATABASE_URL% --model-path models\model.joblib --require-scored-table`
6. Export dashboard-ready CSVs:
   `python scripts\export_dashboard_data.py --db-url %CC_DATABASE_URL% --export-dir exports`

## Repository Notes

- The legacy script name `load_csvs_to_sqlite.py` is kept for compatibility, but it now supports PostgreSQL through `--db-url`.
- The recovery target is a proxy label, so the project should be presented as recovery propensity and account prioritization rather than guaranteed operational truth.
- Model and export artifacts are generated locally and ignored by Git.

See the `notebooks/` folder for analysis instructions and exploratory work.
