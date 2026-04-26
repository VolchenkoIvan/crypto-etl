-- Процедуры переноса данных о монетах из STG в DWH
CREATE OR REPLACE PROCEDURE dwh.load_crypto_prices()
LANGUAGE plpgsql
AS $$
DECLARE
    v_execution_id UUID := gen_random_uuid();
    v_row_cnt INT;
BEGIN

    -- Open connection for logs
    PERFORM dwh.dblink_connect(
        'log_conn',
        'host=localhost dbname=postgres user=logger_user password=strong_password'
    );

    PERFORM dwh.p_execution_log(
        v_execution_id,
        'dwh.load_crypto_prices',
        'START'
    );

    -- INSERT new coins to the DIM
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

    -- Load fact STG->DWH
    INSERT INTO dwh.crypto_prices (
        coin_id
        ,price
        ,market_cap
        ,date_id
    )
    SELECT
        c.id
        ,s.price::numeric
        ,s.market_cap::numeric
        ,s.date_id
    FROM tmp_validation s
    JOIN dwh.coins c
        ON c.name = s.name
       AND c.symbol = s.symbol
    WHERE error_count = 0
    ON CONFLICT (date_id,coin_id) DO NOTHING;

    GET DIAGNOSTICS v_row_cnt = ROW_COUNT;

    insert into stg.crypto_prices_err(
        name
        ,symbol
        ,price
        ,market_cap
        ,date_id
    )
    SELECT
        name
        ,symbol
        ,price
        ,market_cap
        ,date_id
    FROM tmp_validation
    WHERE error_count > 0;

    PERFORM dwh.p_execution_log(
        v_execution_id,
        'dwh.load_crypto_prices',
        'END',
        v_row_cnt
    );
     -- close connection
    PERFORM dwh.dblink_disconnect('log_conn');

    EXCEPTION WHEN OTHERS THEN

    -- ERROR log
    PERFORM dwh.p_execution_log(
        v_execution_id,
        'dwh.load_crypto_prices',
        'ERROR',
        NULL,
        NULL,
        SQLERRM
    );

    PERFORM dwh.dblink_disconnect('log_conn');

    RAISE;

END;
$$;