CREATE TABLE IF NOT EXISTS stg.purchases_history (
    transaction_id UUID,
    full_name      TEXT,
    email          TEXT,
    city           TEXT,
    symbol         TEXT,
    amount         TEXT,
    exchange       TEXT,
    file_date      TEXT                  -- YYYYMMDD
);