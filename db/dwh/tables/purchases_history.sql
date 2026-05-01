CREATE TABLE IF NOT EXISTS dwh.purchases_history (
    date_id          int, -- YYYYMMDD
    transaction_uuid UUID,
    user_id          INT,
    full_name        TEXT,
    email            TEXT,
    city_id          INT,
    coin_id          INT,
    amount           NUMERIC,
    exchange_id      INT,
    source_id        INT
);
CREATE INDEX idx_purchases_history_date_id
ON dwh.purchases_history (date_id);

CREATE INDEX idx_purchases_history_transaction_uuid
ON dwh.purchases_history (transaction_uuid);