-- DWH таблица для хранения данных о монетах
CREATE TABLE IF NOT EXISTS dwh.crypto_prices (
    id SERIAL PRIMARY KEY,
    price NUMERIC,
    market_cap NUMERIC,
    date_id INT,
    hour_id INT,
    coin_id INT
);