-- STG table for crypto information
CREATE TABLE IF NOT EXISTS stg.crypto_prices_err (
    name TEXT,
    symbol TEXT,
    price TEXT,
    market_cap TEXT,
    date_id INT
);