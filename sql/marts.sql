-- Core warehouse layers: dimensions, facts, analytical marts, and dashboard views.

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer AS
SELECT
  a.SK_ID_CURR,
  ROUND(ABS(a.DAYS_BIRTH) / 365.25, 1) AS customer_age_years,
  CASE
    WHEN ABS(a.DAYS_BIRTH) / 365.25 < 30 THEN 'under_30'
    WHEN ABS(a.DAYS_BIRTH) / 365.25 < 40 THEN '30_39'
    WHEN ABS(a.DAYS_BIRTH) / 365.25 < 50 THEN '40_49'
    WHEN ABS(a.DAYS_BIRTH) / 365.25 < 60 THEN '50_59'
    ELSE '60_plus'
  END AS customer_age_band,
  a.AMT_INCOME_TOTAL AS customer_income_total,
  CASE
    WHEN a.AMT_INCOME_TOTAL < 150000 THEN 'income_low'
    WHEN a.AMT_INCOME_TOTAL < 250000 THEN 'income_mid'
    WHEN a.AMT_INCOME_TOTAL < 500000 THEN 'income_upper_mid'
    ELSE 'income_high'
  END AS customer_income_band,
  CASE WHEN a.DAYS_EMPLOYED = 365243 THEN NULL ELSE ROUND(ABS(a.DAYS_EMPLOYED) / 365.25, 1) END AS customer_employment_years,
  CASE
    WHEN a.DAYS_EMPLOYED = 365243 THEN 'missing'
    WHEN ABS(a.DAYS_EMPLOYED) / 365.25 < 1 THEN 'under_1'
    WHEN ABS(a.DAYS_EMPLOYED) / 365.25 < 3 THEN '1_3'
    WHEN ABS(a.DAYS_EMPLOYED) / 365.25 < 7 THEN '3_7'
    ELSE '7_plus'
  END AS customer_employment_band,
  CASE WHEN a.CODE_GENDER = 'M' THEN 'male' WHEN a.CODE_GENDER = 'F' THEN 'female' ELSE 'unknown' END AS customer_gender_band,
  a.CNT_CHILDREN AS customer_children_count,
  a.CNT_FAM_MEMBERS AS customer_family_members_count,
  a.NAME_EDUCATION_TYPE AS customer_education_type,
  a.NAME_FAMILY_STATUS AS customer_family_status,
  a.NAME_HOUSING_TYPE AS customer_housing_type,
  CASE WHEN a.FLAG_OWN_CAR = 'Y' THEN 1 ELSE 0 END AS customer_own_car_flag,
  CASE WHEN a.FLAG_OWN_REALTY = 'Y' THEN 1 ELSE 0 END AS customer_own_realty_flag,
  a.REGION_RATING_CLIENT AS customer_region_rating_client,
  a.NAME_INCOME_TYPE AS customer_income_type
FROM application_train a;

DROP TABLE IF EXISTS dim_loan;
CREATE TABLE dim_loan AS
SELECT
  a.SK_ID_CURR,
  a.NAME_CONTRACT_TYPE AS loan_contract_type,
  a.AMT_CREDIT AS loan_amount,
  a.AMT_ANNUITY AS loan_annuity_amount,
  a.AMT_GOODS_PRICE AS loan_goods_price,
  CASE WHEN a.AMT_INCOME_TOTAL > 0 THEN a.AMT_CREDIT / NULLIF(a.AMT_INCOME_TOTAL, 0) ELSE NULL END AS loan_credit_to_income_ratio,
  CASE WHEN a.AMT_INCOME_TOTAL > 0 THEN a.AMT_ANNUITY / NULLIF(a.AMT_INCOME_TOTAL, 0) ELSE NULL END AS loan_annuity_to_income_ratio,
  CASE WHEN a.AMT_CREDIT > 0 THEN a.AMT_GOODS_PRICE / NULLIF(a.AMT_CREDIT, 0) ELSE NULL END AS loan_goods_to_credit_ratio,
  a.NAME_TYPE_SUITE AS loan_name_type_suite,
  a.NAME_INCOME_TYPE AS loan_income_type,
  a.NAME_EDUCATION_TYPE AS loan_education_type,
  a.NAME_FAMILY_STATUS AS loan_family_status,
  a.NAME_HOUSING_TYPE AS loan_housing_type,
  a.ORGANIZATION_TYPE AS loan_organization_type,
  a.WEEKDAY_APPR_PROCESS_START AS loan_weekday_appr_process_start,
  a.HOUR_APPR_PROCESS_START AS loan_hour_appr_process_start,
  a.OCCUPATION_TYPE AS loan_occupation_type,
  a.CNT_CHILDREN AS loan_children_count,
  a.CNT_FAM_MEMBERS AS loan_family_members_count,
  a.REGION_RATING_CLIENT AS loan_region_rating_client
FROM application_train a;

DROP TABLE IF EXISTS dim_risk_band;
CREATE TABLE dim_risk_band AS
SELECT 1 AS risk_band_rank, 'low risk' AS risk_band, 0.75 AS min_score, 1.00 AS max_score, 'call' AS recommended_action
UNION ALL
SELECT 2 AS risk_band_rank, 'watchlist' AS risk_band, 0.55 AS min_score, 0.7499 AS max_score, 'sms' AS recommended_action
UNION ALL
SELECT 3 AS risk_band_rank, 'high risk' AS risk_band, 0.35 AS min_score, 0.5499 AS max_score, 'field_visit' AS recommended_action
UNION ALL
SELECT 4 AS risk_band_rank, 'critical risk' AS risk_band, 0.00 AS min_score, 0.3499 AS max_score, 'legal' AS recommended_action;

DROP TABLE IF EXISTS dim_channel;
CREATE TABLE dim_channel AS
SELECT 1 AS channel_rank, 'call' AS channel_name, 'call_now' AS channel_default_action
UNION ALL
SELECT 2 AS channel_rank, 'sms' AS channel_name, 'short_text_follow_up' AS channel_default_action
UNION ALL
SELECT 3 AS channel_rank, 'email' AS channel_name, 'email_nudge' AS channel_default_action
UNION ALL
SELECT 4 AS channel_rank, 'field_visit' AS channel_name, 'field_collection_visit' AS channel_default_action
UNION ALL
SELECT 5 AS channel_rank, 'legal' AS channel_name, 'legal_escalation' AS channel_default_action;

DROP TABLE IF EXISTS fact_installment;
CREATE TABLE fact_installment AS
WITH installment_sequence AS (
  SELECT
    ip.SK_ID_CURR,
    ip.SK_ID_PREV,
    ip.NUM_INSTALMENT_VERSION,
    ip.NUM_INSTALMENT_NUMBER,
    ip.DAYS_INSTALMENT,
    ip.DAYS_ENTRY_PAYMENT,
    ip.AMT_INSTALMENT,
    ip.AMT_PAYMENT,
    ROW_NUMBER() OVER (
      PARTITION BY ip.SK_ID_CURR
      ORDER BY ip.NUM_INSTALMENT_NUMBER, ip.DAYS_INSTALMENT, COALESCE(ip.DAYS_ENTRY_PAYMENT, ip.DAYS_INSTALMENT)
    ) AS installment_seq,
    CASE
      WHEN COALESCE(ip.DAYS_ENTRY_PAYMENT, ip.DAYS_INSTALMENT) - ip.DAYS_INSTALMENT > 0
        THEN COALESCE(ip.DAYS_ENTRY_PAYMENT, ip.DAYS_INSTALMENT) - ip.DAYS_INSTALMENT
      ELSE 0
    END AS late_days,
    CASE WHEN ip.AMT_INSTALMENT > 0 THEN ip.AMT_PAYMENT / NULLIF(ip.AMT_INSTALMENT, 0) ELSE NULL END AS payment_ratio,
    CASE WHEN ip.AMT_INSTALMENT > ip.AMT_PAYMENT THEN ip.AMT_INSTALMENT - ip.AMT_PAYMENT ELSE 0 END AS payment_gap
  FROM installments_payments ip
), installment_windows AS (
  SELECT
    installment_sequence.*,
    AVG(late_days) OVER (PARTITION BY SK_ID_CURR ORDER BY installment_seq ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_delay,
    AVG(late_days) OVER (PARTITION BY SK_ID_CURR ORDER BY installment_seq ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_6_delay,
    AVG(late_days) OVER (PARTITION BY SK_ID_CURR ORDER BY installment_seq ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12_delay,
    ROW_NUMBER() OVER (PARTITION BY SK_ID_CURR ORDER BY installment_seq DESC) AS reverse_installment_seq
  FROM installment_sequence
)
SELECT *
FROM installment_windows;

DROP TABLE IF EXISTS fact_collection_event;
CREATE TABLE fact_collection_event AS
SELECT
  SK_ID_CURR,
  SK_ID_PREV,
  installment_seq AS event_sequence,
  late_days,
  payment_ratio,
  payment_gap,
  CASE WHEN late_days > 0 THEN 1 ELSE 0 END AS collection_event_flag,
  CASE
    WHEN late_days = 0 THEN 'current'
    WHEN late_days <= 30 THEN '0_30'
    WHEN late_days <= 60 THEN '31_60'
    WHEN late_days <= 90 THEN '61_90'
    ELSE '90_plus'
  END AS collection_age_bucket
FROM fact_installment;

DROP TABLE IF EXISTS fact_previous_loan;
CREATE TABLE fact_previous_loan AS
SELECT
  pa.SK_ID_CURR,
  pa.SK_ID_PREV,
  pa.NAME_CONTRACT_TYPE,
  pa.NAME_CONTRACT_STATUS,
  pa.NAME_CASH_LOAN_PURPOSE,
  pa.NAME_PAYMENT_TYPE,
  pa.NAME_CLIENT_TYPE,
  pa.NAME_PORTFOLIO,
  pa.NAME_PRODUCT_TYPE,
  pa.NAME_GOODS_CATEGORY,
  pa.NAME_YIELD_GROUP,
  pa.PRODUCT_COMBINATION,
  pa.AMT_APPLICATION,
  pa.AMT_CREDIT,
  pa.AMT_DOWN_PAYMENT,
  pa.AMT_ANNUITY,
  pa.CNT_PAYMENT,
  pa.DAYS_DECISION,
  pa.DAYS_FIRST_DRAWING,
  pa.DAYS_FIRST_DUE,
  pa.DAYS_LAST_DUE_1ST_VERSION,
  pa.DAYS_LAST_DUE,
  pa.DAYS_TERMINATION,
  CASE WHEN pa.NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END AS approved_flag,
  CASE WHEN pa.NAME_CONTRACT_STATUS = 'Refused' THEN 1 ELSE 0 END AS refused_flag,
  CASE WHEN pa.AMT_APPLICATION > 0 THEN pa.AMT_CREDIT / NULLIF(pa.AMT_APPLICATION, 0) ELSE NULL END AS credit_to_application_ratio,
  CASE WHEN pa.AMT_CREDIT > 0 THEN pa.AMT_ANNUITY / NULLIF(pa.AMT_CREDIT, 0) ELSE NULL END AS annuity_to_credit_ratio,
  CASE WHEN pa.AMT_APPLICATION > 0 THEN pa.AMT_DOWN_PAYMENT / NULLIF(pa.AMT_APPLICATION, 0) ELSE NULL END AS down_payment_to_application_ratio,
  CASE WHEN pa.DAYS_DECISION IS NOT NULL THEN ABS(pa.DAYS_DECISION) ELSE NULL END AS decision_recency_days
FROM previous_application pa;

DROP TABLE IF EXISTS fact_bureau_snapshot;
CREATE TABLE fact_bureau_snapshot AS
WITH bureau_balance_summary AS (
  SELECT
    bb.SK_ID_BUREAU,
    COUNT(*) AS bureau_balance_month_count,
    SUM(CASE WHEN bb.STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END) AS bureau_balance_stress_months,
    SUM(CASE WHEN bb.STATUS IN ('C', 'X', '0') THEN 1 ELSE 0 END) AS bureau_balance_stable_months,
    MAX(bb.MONTHS_BALANCE) AS latest_bureau_month,
    MIN(bb.MONTHS_BALANCE) AS earliest_bureau_month
  FROM bureau_balance bb
  GROUP BY bb.SK_ID_BUREAU
)
SELECT
  b.SK_ID_CURR,
  b.SK_ID_BUREAU,
  b.CREDIT_ACTIVE,
  b.CREDIT_CURRENCY,
  b.CREDIT_TYPE,
  b.DAYS_CREDIT,
  b.CREDIT_DAY_OVERDUE,
  b.DAYS_CREDIT_ENDDATE,
  b.DAYS_ENDDATE_FACT,
  b.DAYS_CREDIT_UPDATE,
  b.AMT_CREDIT_MAX_OVERDUE,
  b.CNT_CREDIT_PROLONG,
  b.AMT_CREDIT_SUM,
  b.AMT_CREDIT_SUM_DEBT,
  b.AMT_CREDIT_SUM_LIMIT,
  b.AMT_CREDIT_SUM_OVERDUE,
  b.AMT_ANNUITY,
  COALESCE(s.bureau_balance_month_count, 0) AS bureau_balance_month_count,
  COALESCE(s.bureau_balance_stress_months, 0) AS bureau_balance_stress_months,
  COALESCE(s.bureau_balance_stable_months, 0) AS bureau_balance_stable_months,
  COALESCE(s.latest_bureau_month, 0) AS latest_bureau_month,
  COALESCE(s.earliest_bureau_month, 0) AS earliest_bureau_month,
  CASE WHEN b.AMT_CREDIT_SUM > 0 THEN b.AMT_CREDIT_SUM_DEBT / NULLIF(b.AMT_CREDIT_SUM, 0) ELSE NULL END AS debt_to_credit_ratio,
  CASE WHEN b.CREDIT_DAY_OVERDUE > 0 THEN 1 ELSE 0 END AS bureau_overdue_flag
FROM bureau b
LEFT JOIN bureau_balance_summary s ON b.SK_ID_BUREAU = s.SK_ID_BUREAU;

DROP TABLE IF EXISTS fact_account_monthly_snapshot;
CREATE TABLE fact_account_monthly_snapshot AS
WITH monthly_union AS (
  SELECT
    b.SK_ID_CURR,
    ABS(bb.MONTHS_BALANCE) AS snapshot_month_index,
    'bureau_balance' AS snapshot_source,
    CASE WHEN bb.STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END AS stress_flag,
    CASE WHEN bb.STATUS IN ('1', '2', '3', '4', '5') THEN 1 ELSE 0 END AS overdue_flag,
    0.0 AS balance_amount,
    0.0 AS payment_amount,
    0.0 AS future_installment_count,
    0.0 AS dpd_amount
  FROM bureau_balance bb
  INNER JOIN bureau b ON bb.SK_ID_BUREAU = b.SK_ID_BUREAU
  UNION ALL
  SELECT
    pa.SK_ID_CURR,
    ABS(p.MONTHS_BALANCE) AS snapshot_month_index,
    'pos_cash_balance' AS snapshot_source,
    CASE WHEN COALESCE(p.SK_DPD, 0) > 0 OR COALESCE(p.SK_DPD_DEF, 0) > 0 THEN 1 ELSE 0 END AS stress_flag,
    CASE WHEN COALESCE(p.SK_DPD, 0) > 0 OR COALESCE(p.SK_DPD_DEF, 0) > 0 THEN 1 ELSE 0 END AS overdue_flag,
    CAST(COALESCE(p.CNT_INSTALMENT_FUTURE, 0) AS REAL) AS balance_amount,
    0.0 AS payment_amount,
    CAST(COALESCE(p.CNT_INSTALMENT_FUTURE, 0) AS REAL) AS future_installment_count,
    CAST(COALESCE(p.SK_DPD, 0) + COALESCE(p.SK_DPD_DEF, 0) AS REAL) AS dpd_amount
  FROM pos_cash_balance p
  INNER JOIN previous_application pa ON p.SK_ID_PREV = pa.SK_ID_PREV
  UNION ALL
  SELECT
    pa.SK_ID_CURR,
    ABS(c.MONTHS_BALANCE) AS snapshot_month_index,
    'credit_card_balance' AS snapshot_source,
    CASE WHEN COALESCE(c.SK_DPD, 0) > 0 OR COALESCE(c.SK_DPD_DEF, 0) > 0 THEN 1 ELSE 0 END AS stress_flag,
    CASE WHEN COALESCE(c.SK_DPD, 0) > 0 OR COALESCE(c.SK_DPD_DEF, 0) > 0 THEN 1 ELSE 0 END AS overdue_flag,
    CAST(COALESCE(c.AMT_BALANCE, 0) AS REAL) AS balance_amount,
    CAST(COALESCE(c.AMT_PAYMENT_TOTAL_CURRENT, 0) AS REAL) AS payment_amount,
    CAST(COALESCE(c.CNT_INSTALMENT_MATURE_CUM, 0) AS REAL) AS future_installment_count,
    CAST(COALESCE(c.SK_DPD, 0) + COALESCE(c.SK_DPD_DEF, 0) AS REAL) AS dpd_amount
  FROM credit_card_balance c
  INNER JOIN previous_application pa ON c.SK_ID_PREV = pa.SK_ID_PREV
)
SELECT
  SK_ID_CURR,
  snapshot_month_index,
  COUNT(*) AS snapshot_event_count,
  COUNT(DISTINCT snapshot_source) AS source_diversity_count,
  SUM(stress_flag) AS stress_flag_count,
  SUM(overdue_flag) AS overdue_flag_count,
  SUM(balance_amount) AS balance_amount_sum,
  AVG(balance_amount) AS balance_amount_avg,
  SUM(payment_amount) AS payment_amount_sum,
  AVG(payment_amount) AS payment_amount_avg,
  SUM(future_installment_count) AS future_installment_count_sum,
  AVG(dpd_amount) AS dpd_amount_avg,
  MAX(dpd_amount) AS dpd_amount_max
FROM monthly_union
GROUP BY SK_ID_CURR, snapshot_month_index;

DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time AS
SELECT DISTINCT
  snapshot_month_index AS time_snapshot_month_index,
  CASE
    WHEN snapshot_month_index = 0 THEN 'current'
    WHEN snapshot_month_index BETWEEN 1 AND 3 THEN 'recent_1_3'
    WHEN snapshot_month_index BETWEEN 4 AND 6 THEN 'recent_4_6'
    WHEN snapshot_month_index BETWEEN 7 AND 12 THEN 'recent_7_12'
    ELSE 'historical_13_plus'
  END AS time_month_band,
  CASE WHEN snapshot_month_index <= 3 THEN 1 ELSE 0 END AS time_is_recent_flag,
  CASE WHEN snapshot_month_index <= 12 THEN 1 ELSE 0 END AS time_is_within_year_flag
FROM fact_account_monthly_snapshot;

DROP TABLE IF EXISTS mart_installment_behavior;
CREATE TABLE mart_installment_behavior AS
SELECT
  SK_ID_CURR,
  COUNT(*) AS installment_count,
  SUM(CASE WHEN late_days > 0 THEN 1 ELSE 0 END) AS installment_late_count,
  CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN late_days > 0 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS REAL) ELSE 0 END AS installment_late_ratio,
  MAX(late_days) AS installment_max_delay_days,
  AVG(late_days) AS installment_avg_delay_days,
  SUM(CASE WHEN late_days = 0 THEN 1 ELSE 0 END) AS installment_on_time_count,
  CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN late_days = 0 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS REAL) ELSE 0 END AS installment_on_time_ratio,
  AVG(payment_ratio) AS installment_avg_payment_ratio,
  SUM(payment_ratio) AS installment_sum_payment_ratio,
  SUM(AMT_INSTALMENT) AS installment_total_instalment_amount,
  SUM(AMT_PAYMENT) AS installment_total_payment_amount,
  SUM(payment_gap) AS installment_payment_gap_total,
  SUM(CASE WHEN payment_gap > 0 THEN payment_gap ELSE 0 END) AS installment_outstanding_balance_proxy,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN payment_ratio END) AS installment_latest_payment_ratio,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN late_days END) AS installment_latest_late_days,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN rolling_3_delay END) AS installment_recent_3_delay_avg,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN rolling_6_delay END) AS installment_recent_6_delay_avg,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN rolling_12_delay END) AS installment_recent_12_delay_avg,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN rolling_3_delay END) - MAX(CASE WHEN reverse_installment_seq = 1 THEN rolling_12_delay END) AS installment_delay_trend,
  MAX(CASE WHEN reverse_installment_seq = 1 THEN payment_ratio END) - AVG(payment_ratio) AS installment_payment_ratio_trend
FROM fact_installment
GROUP BY SK_ID_CURR;

DROP TABLE IF EXISTS mart_bureau_history;
CREATE TABLE mart_bureau_history AS
SELECT
  SK_ID_CURR,
  COUNT(*) AS bureau_account_count,
  SUM(CASE WHEN CREDIT_ACTIVE = 'Active' THEN 1 ELSE 0 END) AS bureau_active_count,
  SUM(CASE WHEN CREDIT_ACTIVE = 'Closed' THEN 1 ELSE 0 END) AS bureau_closed_count,
  SUM(bureau_overdue_flag) AS bureau_overdue_count,
  CASE WHEN COUNT(*) > 0 THEN SUM(bureau_overdue_flag) / CAST(COUNT(*) AS REAL) ELSE 0 END AS bureau_overdue_ratio,
  MAX(CREDIT_DAY_OVERDUE) AS bureau_max_overdue_days,
  AVG(CREDIT_DAY_OVERDUE) AS bureau_avg_overdue_days,
  SUM(AMT_CREDIT_SUM) AS bureau_total_credit_sum,
  SUM(AMT_CREDIT_SUM_DEBT) AS bureau_total_debt_sum,
  SUM(AMT_CREDIT_SUM_OVERDUE) AS bureau_total_overdue_sum,
  SUM(AMT_CREDIT_SUM_LIMIT) AS bureau_total_limit_sum,
  AVG(debt_to_credit_ratio) AS bureau_avg_debt_to_credit_ratio,
  SUM(bureau_balance_month_count) AS bureau_balance_month_count,
  SUM(bureau_balance_stress_months) AS bureau_balance_stress_months,
  CASE WHEN SUM(bureau_balance_month_count) > 0 THEN SUM(bureau_balance_stress_months) / CAST(SUM(bureau_balance_month_count) AS REAL) ELSE 0 END AS bureau_balance_stress_ratio,
  MAX(latest_bureau_month) AS bureau_latest_month_index,
  COUNT(DISTINCT CREDIT_TYPE) AS bureau_type_count
FROM fact_bureau_snapshot
GROUP BY SK_ID_CURR;

DROP TABLE IF EXISTS mart_previous_application_summary;
CREATE TABLE mart_previous_application_summary AS
SELECT
  SK_ID_CURR,
  COUNT(*) AS previous_application_count,
  SUM(approved_flag) AS previous_approved_count,
  SUM(refused_flag) AS previous_refused_count,
  CASE WHEN COUNT(*) > 0 THEN SUM(approved_flag) / CAST(COUNT(*) AS REAL) ELSE 0 END AS previous_approval_ratio,
  CASE WHEN COUNT(*) > 0 THEN SUM(refused_flag) / CAST(COUNT(*) AS REAL) ELSE 0 END AS previous_refusal_ratio,
  AVG(credit_to_application_ratio) AS previous_avg_credit_to_application_ratio,
  AVG(annuity_to_credit_ratio) AS previous_avg_annuity_to_credit_ratio,
  AVG(down_payment_to_application_ratio) AS previous_avg_down_payment_ratio,
  MIN(decision_recency_days) AS previous_recent_decision_days,
  AVG(decision_recency_days) AS previous_avg_decision_days,
  MAX(decision_recency_days) - MIN(decision_recency_days) AS previous_decision_spread_days,
  COUNT(DISTINCT NAME_CASH_LOAN_PURPOSE) AS previous_purpose_count,
  COUNT(DISTINCT NAME_CONTRACT_TYPE) AS previous_contract_type_count,
  COUNT(DISTINCT NAME_CLIENT_TYPE) AS previous_client_type_count
FROM fact_previous_loan
GROUP BY SK_ID_CURR;

DROP TABLE IF EXISTS mart_monthly_behavior;
CREATE TABLE mart_monthly_behavior AS
WITH monthly_ranked AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY SK_ID_CURR ORDER BY snapshot_month_index DESC) AS month_seq_desc,
    AVG(CASE WHEN stress_flag_count > 0 THEN 1.0 ELSE 0.0 END) OVER (
      PARTITION BY SK_ID_CURR ORDER BY snapshot_month_index ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_stress_rate,
    AVG(CASE WHEN stress_flag_count > 0 THEN 1.0 ELSE 0.0 END) OVER (
      PARTITION BY SK_ID_CURR ORDER BY snapshot_month_index ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_6_stress_rate,
    AVG(CASE WHEN stress_flag_count > 0 THEN 1.0 ELSE 0.0 END) OVER (
      PARTITION BY SK_ID_CURR ORDER BY snapshot_month_index ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_12_stress_rate
  FROM fact_account_monthly_snapshot f
)
SELECT
  SK_ID_CURR,
  COUNT(*) AS monthly_snapshot_count,
  SUM(stress_flag_count) AS monthly_stress_events,
  SUM(overdue_flag_count) AS monthly_overdue_events,
  AVG(balance_amount_avg) AS monthly_avg_balance,
  SUM(payment_amount_sum) AS monthly_total_payment,
  SUM(future_installment_count_sum) AS monthly_total_future_installment_count,
  MAX(snapshot_month_index) AS monthly_latest_snapshot_month_index,
  MAX(source_diversity_count) AS monthly_source_diversity_max,
  MAX(CASE WHEN month_seq_desc = 1 THEN rolling_3_stress_rate END) AS monthly_recent_3_stress_rate,
  MAX(CASE WHEN month_seq_desc = 1 THEN rolling_6_stress_rate END) AS monthly_recent_6_stress_rate,
  MAX(CASE WHEN month_seq_desc = 1 THEN rolling_12_stress_rate END) AS monthly_recent_12_stress_rate,
  MAX(CASE WHEN month_seq_desc = 1 THEN rolling_3_stress_rate END) - MAX(CASE WHEN month_seq_desc = 1 THEN rolling_12_stress_rate END) AS monthly_stress_trend,
  CASE WHEN COUNT(*) > 0 THEN SUM(overdue_flag_count) / CAST(COUNT(*) AS REAL) ELSE 0 END AS monthly_overdue_ratio
FROM monthly_ranked
GROUP BY SK_ID_CURR;

DROP TABLE IF EXISTS mart_recovery_labels;
CREATE TABLE mart_recovery_labels AS
SELECT
  ib.SK_ID_CURR,
  CASE WHEN ib.installment_late_count > 0 THEN 1 ELSE 0 END AS collections_scope_flag,
  CASE
    WHEN ib.installment_late_count = 0 THEN NULL
    WHEN ib.installment_late_ratio <= 0.10
       AND ib.installment_max_delay_days <= 30
       AND COALESCE(ib.installment_recent_3_delay_avg, 0) <= 5 THEN 1
    WHEN ib.installment_late_ratio <= 0.25
       AND ib.installment_max_delay_days <= 60
       AND COALESCE(ib.installment_recent_6_delay_avg, 0) <= 10 THEN 1
    ELSE 0
  END AS proxy_recovery_label,
  CASE
    WHEN ib.installment_late_count = 0 THEN 'not_in_collections_scope'
    WHEN ib.installment_max_delay_days <= 30 THEN 'short_delinquency'
    WHEN ib.installment_max_delay_days <= 60 THEN 'medium_delinquency'
    WHEN ib.installment_max_delay_days <= 90 THEN 'severe_delinquency'
    ELSE 'critical_delinquency'
  END AS delinquency_age_bucket,
  CASE
    WHEN ib.installment_late_count = 0 THEN 'good_standing'
    WHEN ib.installment_late_ratio <= 0.10 AND ib.installment_max_delay_days <= 30 THEN 'likely_recovered'
    WHEN ib.installment_late_ratio <= 0.25 AND ib.installment_max_delay_days <= 60 THEN 'partially_recovered'
    ELSE 'not_recovered'
  END AS recovery_label_reason
FROM mart_installment_behavior ib;

DROP TABLE IF EXISTS mart_model_features;
CREATE TABLE mart_model_features AS
SELECT
  dc.SK_ID_CURR,
  dc.customer_age_years,
  dc.customer_age_band,
  dc.customer_income_total,
  dc.customer_income_band,
  dc.customer_employment_years,
  dc.customer_employment_band,
  dc.customer_gender_band,
  dc.customer_children_count,
  dc.customer_family_members_count,
  dc.customer_education_type,
  dc.customer_family_status,
  dc.customer_housing_type,
  dc.customer_own_car_flag,
  dc.customer_own_realty_flag,
  dc.customer_region_rating_client,
  dc.customer_income_type,
  dl.loan_contract_type,
  dl.loan_amount,
  dl.loan_annuity_amount,
  dl.loan_goods_price,
  dl.loan_credit_to_income_ratio,
  dl.loan_annuity_to_income_ratio,
  dl.loan_goods_to_credit_ratio,
  dl.loan_name_type_suite,
  dl.loan_income_type,
  dl.loan_education_type,
  dl.loan_family_status,
  dl.loan_housing_type,
  dl.loan_organization_type,
  dl.loan_weekday_appr_process_start,
  dl.loan_hour_appr_process_start,
  dl.loan_occupation_type,
  dl.loan_children_count,
  dl.loan_family_members_count,
  dl.loan_region_rating_client,
  ib.installment_count,
  ib.installment_late_count,
  ib.installment_late_ratio,
  ib.installment_max_delay_days,
  ib.installment_avg_delay_days,
  ib.installment_on_time_count,
  ib.installment_on_time_ratio,
  ib.installment_avg_payment_ratio,
  ib.installment_sum_payment_ratio,
  ib.installment_total_instalment_amount,
  ib.installment_total_payment_amount,
  ib.installment_payment_gap_total,
  ib.installment_outstanding_balance_proxy,
  ib.installment_latest_payment_ratio,
  ib.installment_latest_late_days,
  ib.installment_recent_3_delay_avg,
  ib.installment_recent_6_delay_avg,
  ib.installment_recent_12_delay_avg,
  ib.installment_delay_trend,
  ib.installment_payment_ratio_trend,
  bh.bureau_account_count,
  bh.bureau_active_count,
  bh.bureau_closed_count,
  bh.bureau_overdue_count,
  bh.bureau_overdue_ratio,
  bh.bureau_max_overdue_days,
  bh.bureau_avg_overdue_days,
  bh.bureau_total_credit_sum,
  bh.bureau_total_debt_sum,
  bh.bureau_total_overdue_sum,
  bh.bureau_total_limit_sum,
  bh.bureau_avg_debt_to_credit_ratio,
  bh.bureau_balance_month_count,
  bh.bureau_balance_stress_months,
  bh.bureau_balance_stress_ratio,
  bh.bureau_latest_month_index,
  bh.bureau_type_count,
  pa.previous_application_count,
  pa.previous_approved_count,
  pa.previous_refused_count,
  pa.previous_approval_ratio,
  pa.previous_refusal_ratio,
  pa.previous_avg_credit_to_application_ratio,
  pa.previous_avg_annuity_to_credit_ratio,
  pa.previous_avg_down_payment_ratio,
  pa.previous_recent_decision_days,
  pa.previous_avg_decision_days,
  pa.previous_decision_spread_days,
  pa.previous_purpose_count,
  pa.previous_contract_type_count,
  pa.previous_client_type_count,
  mb.monthly_snapshot_count,
  mb.monthly_stress_events,
  mb.monthly_overdue_events,
  mb.monthly_avg_balance,
  mb.monthly_total_payment,
  mb.monthly_total_future_installment_count,
  mb.monthly_latest_snapshot_month_index,
  mb.monthly_source_diversity_max,
  mb.monthly_recent_3_stress_rate,
  mb.monthly_recent_6_stress_rate,
  mb.monthly_recent_12_stress_rate,
  mb.monthly_stress_trend,
  mb.monthly_overdue_ratio,
  rl.collections_scope_flag,
  rl.proxy_recovery_label,
  rl.delinquency_age_bucket,
  rl.recovery_label_reason,
  CASE
    WHEN ib.installment_late_count = 0 THEN 0.84
    WHEN ib.installment_late_ratio <= 0.10
       AND COALESCE(bh.bureau_overdue_ratio, 0) <= 0.10
       AND COALESCE(mb.monthly_recent_3_stress_rate, 0) <= 0.25 THEN 0.76
    WHEN ib.installment_max_delay_days <= 30
       AND COALESCE(mb.monthly_recent_3_stress_rate, 0) <= 0.40 THEN 0.62
    WHEN ib.installment_max_delay_days <= 60
       OR COALESCE(bh.bureau_overdue_ratio, 0) <= 0.25 THEN 0.42
    ELSE 0.18
  END AS recovery_propensity_baseline,
  CASE
    WHEN ib.installment_max_delay_days = 0 THEN '0_current'
    WHEN ib.installment_max_delay_days <= 30 THEN '1_0_30'
    WHEN ib.installment_max_delay_days <= 60 THEN '2_31_60'
    WHEN ib.installment_max_delay_days <= 90 THEN '3_61_90'
    ELSE '4_90_plus'
  END AS aging_bucket,
  CASE
    WHEN ib.installment_late_count = 0 THEN 'low risk'
    WHEN ib.installment_late_ratio <= 0.10
       AND COALESCE(bh.bureau_overdue_ratio, 0) <= 0.10 THEN 'watchlist'
    WHEN ib.installment_max_delay_days <= 60
       OR COALESCE(bh.bureau_overdue_ratio, 0) <= 0.25 THEN 'high risk'
    ELSE 'critical risk'
  END AS risk_band,
  CASE
    WHEN ib.installment_late_count = 0 THEN 'email'
    WHEN ib.installment_late_ratio <= 0.10 THEN 'sms'
    WHEN ib.installment_max_delay_days <= 60 THEN 'call'
    WHEN ib.installment_max_delay_days <= 90 THEN 'field_visit'
    ELSE 'legal'
  END AS channel_recommendation,
  CASE
    WHEN ib.installment_late_count = 0 THEN 'monitor'
    WHEN ib.installment_max_delay_days <= 30 THEN 'frontline'
    WHEN ib.installment_max_delay_days <= 60 THEN 'priority'
    WHEN ib.installment_max_delay_days <= 90 THEN 'urgent'
    ELSE 'critical'
  END AS queue_band,
  CASE
    WHEN COALESCE(ib.installment_outstanding_balance_proxy, 0) > 0 THEN ib.installment_outstanding_balance_proxy
    ELSE COALESCE(dl.loan_amount, 0)
  END AS outstanding_balance_proxy,
  CASE
    WHEN ib.installment_late_count = 0 THEN 'green'
    WHEN ib.installment_max_delay_days <= 30 THEN 'amber'
    WHEN ib.installment_max_delay_days <= 60 THEN 'orange'
    ELSE 'red'
  END AS severity_band
FROM dim_customer dc
INNER JOIN dim_loan dl ON dc.SK_ID_CURR = dl.SK_ID_CURR
LEFT JOIN mart_installment_behavior ib ON dc.SK_ID_CURR = ib.SK_ID_CURR
LEFT JOIN mart_bureau_history bh ON dc.SK_ID_CURR = bh.SK_ID_CURR
LEFT JOIN mart_previous_application_summary pa ON dc.SK_ID_CURR = pa.SK_ID_CURR
LEFT JOIN mart_monthly_behavior mb ON dc.SK_ID_CURR = mb.SK_ID_CURR
LEFT JOIN mart_recovery_labels rl ON dc.SK_ID_CURR = rl.SK_ID_CURR
WHERE rl.proxy_recovery_label IS NOT NULL;

DROP TABLE IF EXISTS mart_delinquency_aging;
CREATE TABLE mart_delinquency_aging AS
SELECT
  SK_ID_CURR,
  installment_max_delay_days,
  installment_late_count,
  installment_late_ratio,
  aging_bucket,
  severity_band,
  outstanding_balance_proxy,
  recovery_propensity_baseline,
  CASE
    WHEN installment_late_count = 0 THEN 'current'
    WHEN installment_max_delay_days <= 30 THEN '0_30'
    WHEN installment_max_delay_days <= 60 THEN '31_60'
    WHEN installment_max_delay_days <= 90 THEN '61_90'
    ELSE '90_plus'
  END AS aging_bucket_label,
  CASE
    WHEN installment_late_count = 0 THEN 1
    WHEN installment_max_delay_days <= 30 THEN 2
    WHEN installment_max_delay_days <= 60 THEN 3
    WHEN installment_max_delay_days <= 90 THEN 4
    ELSE 5
  END AS aging_bucket_rank
FROM mart_model_features;

DROP TABLE IF EXISTS mart_recovery_score;
CREATE TABLE mart_recovery_score AS
SELECT
  SK_ID_CURR,
  recovery_propensity_baseline AS recovery_propensity,
  expected_recovery_value_proxy,
  priority_score_baseline,
  risk_band,
  aging_bucket,
  queue_band,
  channel_recommendation
FROM (
  SELECT
    mf.*,
    CASE
      WHEN mf.installment_late_count = 0 THEN COALESCE(mf.outstanding_balance_proxy, 0) * mf.recovery_propensity_baseline
      ELSE mf.outstanding_balance_proxy * mf.recovery_propensity_baseline
    END AS expected_recovery_value_proxy,
    CASE
      WHEN COALESCE(mf.outstanding_balance_proxy, 0) > 0 THEN COALESCE(mf.outstanding_balance_proxy, 0) * mf.recovery_propensity_baseline
      ELSE COALESCE(mf.loan_amount, 0) * mf.recovery_propensity_baseline
    END * (1 + COALESCE(mf.installment_max_delay_days, 0) / 90.0) * (1 + COALESCE(mf.monthly_recent_3_stress_rate, 0)) AS priority_score_baseline
  FROM mart_model_features mf
) scored;

DROP TABLE IF EXISTS mart_contact_priority;
CREATE TABLE mart_contact_priority AS
SELECT
  SK_ID_CURR,
  priority_rank,
  priority_decile,
  proxy_recovery_label,
  recovery_propensity_baseline,
  expected_recovery_value_proxy,
  priority_score_baseline,
  risk_band,
  queue_band,
  channel_recommendation,
  aging_bucket,
  severity_band,
  installment_max_delay_days,
  installment_late_count,
  installment_outstanding_balance_proxy,
  monthly_recent_3_stress_rate,
  monthly_recent_6_stress_rate,
  monthly_recent_12_stress_rate,
  monthly_stress_trend,
  bureau_overdue_ratio,
  previous_approval_ratio,
  customer_age_band,
  customer_income_band,
  loan_contract_type,
  loan_amount,
  loan_annuity_amount,
  CASE
    WHEN risk_band = 'critical risk' THEN 'legal_escalation'
    WHEN risk_band = 'high risk' THEN 'field_visit'
    WHEN risk_band = 'watchlist' THEN 'sms_follow_up'
    ELSE 'call_follow_up'
  END AS next_best_action
FROM (
  SELECT
    mf.*,
    ROW_NUMBER() OVER (
      ORDER BY mf.priority_score_baseline DESC, mf.expected_recovery_value_proxy DESC, mf.installment_max_delay_days DESC
    ) AS priority_rank,
    NTILE(10) OVER (ORDER BY mf.priority_score_baseline DESC) AS priority_decile
  FROM (
    SELECT
      mff.*,
      CASE
        WHEN COALESCE(mff.outstanding_balance_proxy, 0) > 0 THEN COALESCE(mff.outstanding_balance_proxy, 0) * mff.recovery_propensity_baseline
        ELSE COALESCE(mff.loan_amount, 0) * mff.recovery_propensity_baseline
      END AS expected_recovery_value_proxy,
      CASE
        WHEN COALESCE(mff.outstanding_balance_proxy, 0) > 0 THEN COALESCE(mff.outstanding_balance_proxy, 0) * mff.recovery_propensity_baseline
        ELSE COALESCE(mff.loan_amount, 0) * mff.recovery_propensity_baseline
      END * (1 + COALESCE(mff.installment_max_delay_days, 0) / 90.0) * (1 + COALESCE(mff.monthly_recent_3_stress_rate, 0)) AS priority_score_baseline
    FROM mart_model_features mff
  ) mf
) ranked;

DROP VIEW IF EXISTS vw_kpi_summary;
CREATE VIEW vw_kpi_summary AS
SELECT
  risk_band,
  aging_bucket,
  queue_band,
  channel_recommendation,
  COUNT(*) AS account_count,
  SUM(expected_recovery_value_proxy) AS expected_recovery_value,
  AVG(recovery_propensity_baseline) AS avg_recovery_propensity,
  AVG(priority_score_baseline) AS avg_priority_score,
  SUM(CASE WHEN proxy_recovery_label = 1 THEN 1 ELSE 0 END) AS recoverable_account_count,
  SUM(CASE WHEN proxy_recovery_label = 1 THEN expected_recovery_value_proxy ELSE 0 END) AS recoverable_value
FROM mart_contact_priority
GROUP BY risk_band, aging_bucket, queue_band, channel_recommendation;

DROP VIEW IF EXISTS vw_recovery_cohort_summary;
CREATE VIEW vw_recovery_cohort_summary AS
SELECT
  customer_age_band,
  customer_income_band,
  loan_contract_type,
  risk_band,
  aging_bucket,
  COUNT(*) AS account_count,
  AVG(recovery_propensity_baseline) AS avg_recovery_propensity,
  AVG(priority_score_baseline) AS avg_priority_score,
  SUM(expected_recovery_value_proxy) AS expected_recovery_value,
  SUM(CASE WHEN proxy_recovery_label = 1 THEN 1 ELSE 0 END) AS recovered_accounts
FROM mart_contact_priority
GROUP BY customer_age_band, customer_income_band, loan_contract_type, risk_band, aging_bucket;

DROP VIEW IF EXISTS vw_channel_summary;
CREATE VIEW vw_channel_summary AS
SELECT
  channel_recommendation,
  queue_band,
  COUNT(*) AS account_count,
  SUM(expected_recovery_value_proxy) AS expected_recovery_value,
  AVG(recovery_propensity_baseline) AS avg_recovery_propensity,
  AVG(priority_score_baseline) AS avg_priority_score
FROM mart_contact_priority
GROUP BY channel_recommendation, queue_band;

CREATE INDEX IF NOT EXISTS idx_fact_account_monthly_snapshot_sk_id_curr
  ON fact_account_monthly_snapshot(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_mart_model_features_sk_id_curr
  ON mart_model_features(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_mart_contact_priority_priority_rank
  ON mart_contact_priority(priority_rank);

CREATE INDEX IF NOT EXISTS idx_mart_contact_priority_sk_id_curr
  ON mart_contact_priority(SK_ID_CURR);
