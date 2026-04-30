# Analysis & Notebook Instructions

1. Start a notebook server with the same conda environment:
   `python -m notebook`
2. Connect the notebook to the warehouse through SQLAlchemy.
3. Recommended flow:
   - Inspect the feature mart: `pd.read_sql_query('SELECT * FROM mart_model_features LIMIT 5', engine)`
   - Review the queue: `pd.read_sql_query('SELECT * FROM mart_contact_priority LIMIT 20', engine)`
   - Check the risk slices: `pd.read_sql_query('SELECT * FROM vw_kpi_summary', engine)`
   - Pull cohort-style cuts: `pd.read_sql_query('SELECT * FROM vw_recovery_cohort_summary', engine)`
   - Plot aging and score distributions from `mart_delinquency_aging` and `mart_scored_accounts`
   - Compare the baseline recovery score with the model score and inspect the top-decile queue

4. Use `sql/marts.sql` and `sql/quality_checks.sql` as the canonical definitions for the warehouse layers and validation views.
