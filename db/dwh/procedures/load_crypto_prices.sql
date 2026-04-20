-- Процедуры переноса данных о монетах из STG в DWH
CREATE OR REPLACE PROCEDURE dwh.load_crypto_prices()
LANGUAGE plpgsql
AS $$
BEGIN

    -- 1. вставляем новые монеты в справочник
    INSERT INTO dwh.coins (name, symbol)
    SELECT DISTINCT s.name, s.symbol
    FROM stg.crypto_prices s
    LEFT JOIN dwh.coins c
        ON c.name = s.name AND c.symbol = s.symbol
    WHERE c.id IS NULL;


    -- 2. грузим факт с join на справочник
    INSERT INTO dwh.crypto_prices (
        coin_id,
        price,
        market_cap,
        date_id,
        hour_id
    )
    SELECT
        c.id,
        s.price,
        s.market_cap,
        s.date_id,
        s.hour_id
    FROM stg.crypto_prices s
    JOIN dwh.coins c
        ON c.name = s.name
       AND c.symbol = s.symbol;

    TRUNCATE TABLE stg.crypto_prices;
END;
$$;