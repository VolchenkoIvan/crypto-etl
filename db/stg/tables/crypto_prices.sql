-- STG таблица для хранения данных о монетах
CREATE TABLE IF NOT EXISTS stg.crypto_prices (
    name TEXT,
    symbol TEXT,
    price TEXT,
    market_cap TEXT,
    date_id INT
);