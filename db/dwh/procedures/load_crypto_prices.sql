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

	DROP TABLE IF EXISTS tmp_validation;
    CREATE TEMP TABLE tmp_validation AS
	SELECT
	    *,
	    (
	        CASE
	            WHEN price ~ '^-?\d+(\.\d+)?$' THEN 0
	            ELSE 1
	        END +
	        CASE
	            WHEN market_cap ~ '^-?\d+(\.\d+)?$' THEN 0
	            ELSE 1
	        END
	    ) AS error_count
	FROM stg.crypto_prices;

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
        s.price::numeric,
        s.market_cap::numeric,
        s.date_id,
        s.hour_id
    FROM tmp_validation s
    JOIN dwh.coins c
        ON c.name = s.name
       AND c.symbol = s.symbol
    WHERE error_count = 0;

    --TRUNCATE TABLE stg.crypto_prices;
    insert into stg.crypto_prices_err(
        name
        ,symbol
        ,price
        ,market_cap
        ,date_id
        ,hour_id
    )
    SELECT
        name
        ,symbol
        ,price
        ,market_cap
        ,date_id
        ,hour_id
    FROM tmp_validation
    WHERE error_count > 0;
END;
$$;