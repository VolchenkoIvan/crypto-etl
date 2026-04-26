-- DWH table for crypto information
CREATE TABLE IF NOT EXISTS dwh.crypto_prices (
    price NUMERIC,
    market_cap NUMERIC,
    date_id INT NOT NULL,
    coin_id INT NOT NULL
);
ALTER TABLE dwh.crypto_prices
ADD CONSTRAINT uq_crypto UNIQUE (date_id,coin_id);