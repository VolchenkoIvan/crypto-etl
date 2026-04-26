-- STG table for crypto information (for error rows)
CREATE TABLE IF NOT EXISTS stg.crypto_prices (
    name TEXT,
    symbol TEXT,
    price TEXT,
    market_cap TEXT,
    date_id INT
);