-- Staging-layer indexes for faster joins and summary builds.

CREATE INDEX IF NOT EXISTS idx_application_train_sk_id_curr
    ON application_train(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_application_test_sk_id_curr
    ON application_test(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_previous_application_sk_id_curr
    ON previous_application(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_previous_application_sk_id_prev
    ON previous_application(SK_ID_PREV);

CREATE INDEX IF NOT EXISTS idx_bureau_sk_id_curr
    ON bureau(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_bureau_sk_id_bureau
    ON bureau(SK_ID_BUREAU);

CREATE INDEX IF NOT EXISTS idx_bureau_balance_sk_id_bureau_months_balance
    ON bureau_balance(SK_ID_BUREAU, MONTHS_BALANCE);

CREATE INDEX IF NOT EXISTS idx_installments_payments_sk_id_curr
    ON installments_payments(SK_ID_CURR);

CREATE INDEX IF NOT EXISTS idx_installments_payments_sk_id_prev
    ON installments_payments(SK_ID_PREV);

CREATE INDEX IF NOT EXISTS idx_pos_cash_balance_sk_id_prev_months_balance
    ON pos_cash_balance(SK_ID_PREV, MONTHS_BALANCE);

CREATE INDEX IF NOT EXISTS idx_credit_card_balance_sk_id_prev_months_balance
    ON credit_card_balance(SK_ID_PREV, MONTHS_BALANCE);