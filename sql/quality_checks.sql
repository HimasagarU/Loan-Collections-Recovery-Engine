-- Data quality checks built as views so they can be queried from validation and exports.

DROP VIEW IF EXISTS dq_table_row_counts;
CREATE VIEW dq_table_row_counts AS
SELECT 'application_train' AS table_name, COUNT(*) AS row_count FROM application_train
UNION ALL
SELECT 'application_test' AS table_name, COUNT(*) AS row_count FROM application_test
UNION ALL
SELECT 'previous_application' AS table_name, COUNT(*) AS row_count FROM previous_application
UNION ALL
SELECT 'bureau' AS table_name, COUNT(*) AS row_count FROM bureau
UNION ALL
SELECT 'bureau_balance' AS table_name, COUNT(*) AS row_count FROM bureau_balance
UNION ALL
SELECT 'installments_payments' AS table_name, COUNT(*) AS row_count FROM installments_payments
UNION ALL
SELECT 'pos_cash_balance' AS table_name, COUNT(*) AS row_count FROM pos_cash_balance
UNION ALL
SELECT 'credit_card_balance' AS table_name, COUNT(*) AS row_count FROM credit_card_balance;

DROP VIEW IF EXISTS dq_duplicate_keys;
CREATE VIEW dq_duplicate_keys AS
SELECT 'application_train' AS table_name, 'SK_ID_CURR' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_CURR
    FROM application_train
    GROUP BY SK_ID_CURR
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'application_test' AS table_name, 'SK_ID_CURR' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_CURR
    FROM application_test
    GROUP BY SK_ID_CURR
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'previous_application' AS table_name, 'SK_ID_PREV' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_PREV
    FROM previous_application
    GROUP BY SK_ID_PREV
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'bureau' AS table_name, 'SK_ID_BUREAU' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_BUREAU
    FROM bureau
    GROUP BY SK_ID_BUREAU
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'bureau_balance' AS table_name, 'SK_ID_BUREAU+MONTHS_BALANCE' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_BUREAU, MONTHS_BALANCE
    FROM bureau_balance
    GROUP BY SK_ID_BUREAU, MONTHS_BALANCE
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'installments_payments' AS table_name, 'SK_ID_CURR+SK_ID_PREV+NUM_INSTALMENT_NUMBER' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_CURR, SK_ID_PREV, NUM_INSTALMENT_NUMBER
    FROM installments_payments
    GROUP BY SK_ID_CURR, SK_ID_PREV, NUM_INSTALMENT_NUMBER
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'pos_cash_balance' AS table_name, 'SK_ID_PREV+MONTHS_BALANCE' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_PREV, MONTHS_BALANCE
    FROM pos_cash_balance
    GROUP BY SK_ID_PREV, MONTHS_BALANCE
    HAVING COUNT(*) > 1
) duplicates
UNION ALL
SELECT 'credit_card_balance' AS table_name, 'SK_ID_PREV+MONTHS_BALANCE' AS key_name, COUNT(*) AS duplicate_groups
FROM (
    SELECT SK_ID_PREV, MONTHS_BALANCE
    FROM credit_card_balance
    GROUP BY SK_ID_PREV, MONTHS_BALANCE
    HAVING COUNT(*) > 1
) duplicates;

DROP VIEW IF EXISTS dq_null_key_rows;
CREATE VIEW dq_null_key_rows AS
SELECT 'application_train' AS table_name, 'SK_ID_CURR' AS key_name, SUM(CASE WHEN SK_ID_CURR IS NULL THEN 1 ELSE 0 END) AS null_rows FROM application_train
UNION ALL
SELECT 'application_test' AS table_name, 'SK_ID_CURR' AS key_name, SUM(CASE WHEN SK_ID_CURR IS NULL THEN 1 ELSE 0 END) AS null_rows FROM application_test
UNION ALL
SELECT 'previous_application' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS null_rows FROM previous_application
UNION ALL
SELECT 'bureau' AS table_name, 'SK_ID_BUREAU' AS key_name, SUM(CASE WHEN SK_ID_BUREAU IS NULL THEN 1 ELSE 0 END) AS null_rows FROM bureau
UNION ALL
SELECT 'bureau_balance' AS table_name, 'SK_ID_BUREAU' AS key_name, SUM(CASE WHEN SK_ID_BUREAU IS NULL THEN 1 ELSE 0 END) AS null_rows FROM bureau_balance
UNION ALL
SELECT 'installments_payments' AS table_name, 'SK_ID_CURR' AS key_name, SUM(CASE WHEN SK_ID_CURR IS NULL THEN 1 ELSE 0 END) AS null_rows FROM installments_payments
UNION ALL
SELECT 'installments_payments' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS null_rows FROM installments_payments
UNION ALL
SELECT 'pos_cash_balance' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS null_rows FROM pos_cash_balance
UNION ALL
SELECT 'credit_card_balance' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS null_rows FROM credit_card_balance;

DROP VIEW IF EXISTS dq_orphan_keys;
CREATE VIEW dq_orphan_keys AS
SELECT 'installments_payments' AS table_name, 'SK_ID_CURR' AS key_name, SUM(CASE WHEN a.SK_ID_CURR IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM installments_payments ip
LEFT JOIN application_train a ON ip.SK_ID_CURR = a.SK_ID_CURR
UNION ALL
SELECT 'installments_payments' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN pa.SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM installments_payments ip
LEFT JOIN previous_application pa ON ip.SK_ID_PREV = pa.SK_ID_PREV
UNION ALL
SELECT 'bureau' AS table_name, 'SK_ID_CURR' AS key_name, SUM(CASE WHEN a.SK_ID_CURR IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM bureau b
LEFT JOIN application_train a ON b.SK_ID_CURR = a.SK_ID_CURR
UNION ALL
SELECT 'bureau_balance' AS table_name, 'SK_ID_BUREAU' AS key_name, SUM(CASE WHEN b.SK_ID_BUREAU IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM bureau_balance bb
LEFT JOIN bureau b ON bb.SK_ID_BUREAU = b.SK_ID_BUREAU
UNION ALL
SELECT 'previous_application' AS table_name, 'SK_ID_CURR' AS key_name, SUM(CASE WHEN a.SK_ID_CURR IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM previous_application pa
LEFT JOIN application_train a ON pa.SK_ID_CURR = a.SK_ID_CURR
UNION ALL
SELECT 'pos_cash_balance' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN pa.SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM pos_cash_balance p
LEFT JOIN previous_application pa ON p.SK_ID_PREV = pa.SK_ID_PREV
UNION ALL
SELECT 'credit_card_balance' AS table_name, 'SK_ID_PREV' AS key_name, SUM(CASE WHEN pa.SK_ID_PREV IS NULL THEN 1 ELSE 0 END) AS orphan_rows
FROM credit_card_balance c
LEFT JOIN previous_application pa ON c.SK_ID_PREV = pa.SK_ID_PREV;

DROP VIEW IF EXISTS vw_data_quality_summary;
CREATE VIEW vw_data_quality_summary AS
SELECT 'row_counts' AS check_group, table_name, NULL AS key_name, row_count AS failed_rows,
       CASE WHEN row_count > 0 THEN 'pass' ELSE 'fail' END AS status,
       CASE WHEN row_count > 0 THEN 'table loaded' ELSE 'table is empty' END AS details
FROM dq_table_row_counts
UNION ALL
SELECT 'duplicate_keys' AS check_group, table_name, key_name, duplicate_groups AS failed_rows,
       CASE WHEN duplicate_groups = 0 THEN 'pass' ELSE 'fail' END AS status,
       CASE WHEN duplicate_groups = 0 THEN 'keys are unique' ELSE 'duplicate key groups found' END AS details
FROM dq_duplicate_keys
UNION ALL
SELECT 'null_key_rows' AS check_group, table_name, key_name, null_rows AS failed_rows,
       CASE WHEN null_rows = 0 THEN 'pass' ELSE 'fail' END AS status,
       CASE WHEN null_rows = 0 THEN 'no null keys detected' ELSE 'null key rows found' END AS details
FROM dq_null_key_rows
UNION ALL
SELECT 'orphan_keys' AS check_group, table_name, key_name, orphan_rows AS failed_rows,
       CASE WHEN orphan_rows = 0 THEN 'pass' ELSE 'fail' END AS status,
       CASE WHEN orphan_rows = 0 THEN 'all foreign keys resolved' ELSE 'orphan keys detected' END AS details
FROM dq_orphan_keys;