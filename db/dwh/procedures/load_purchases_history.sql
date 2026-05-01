-- Процедуры переноса данных о монетах из STG в DWH
CREATE OR REPLACE PROCEDURE dwh.load_purchases_history()
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
        'dwh.load_purchases_history',
        'START'
    );

    -- INSERT new cities to the DIM
    INSERT INTO dwh.cities (name)
    SELECT DISTINCT s.city
    FROM stg.purchases_history s
    LEFT JOIN dwh.cities c
        ON c.name = s.city
    WHERE c.id IS NULL;

    -- INSERT new exchanges to the DIM
    INSERT INTO dwh.exchanges (exchange)
    SELECT DISTINCT s.exchange
    FROM stg.purchases_history s
    LEFT JOIN dwh.exchanges c
        ON c.exchange = s.exchange
    WHERE c.id IS NULL;

    -- INSERT new sources to the DIM
    INSERT INTO dwh.sources (source_name)
    SELECT DISTINCT s.source
    FROM stg.purchases_history s
    LEFT JOIN dwh.sources c
        ON c.source_name = s.source
    WHERE c.id IS NULL;

    -- INSERT new users to the DIM
    INSERT INTO dwh.users (full_name, email)
    SELECT DISTINCT s.full_name, s.email
    FROM stg.purchases_history s
    LEFT JOIN dwh.users c
        ON c.full_name = s.full_name AND c.email = s.email
    WHERE c.id IS NULL;

	 -- Load fact STG->DWH
    INSERT INTO dwh.purchases_history (
        date_id
        ,transaction_uuid
        ,user_id
        ,full_name
        ,email
        ,city_id
        ,coin_id
        ,amount
        ,exchange_id
        ,source_id
    )
    SELECT
        s.date_id::int
        ,s.transaction_id
        ,u.id
        ,s.full_name
        ,s.email
        ,ct.id
        ,c.id
        ,s.amount::numeric
        ,e.id
        ,sr.id
    FROM stg.purchases_history s
        INNER JOIN dwh.coins c
            ON c.symbol = s.symbol
        INNER JOIN dwh.users u
            ON u.email = s.email
                AND u.full_name = s.full_name
        INNER JOIN dwh.cities ct
            ON ct.name = s.city
        INNER JOIN dwh.exchanges e
            ON e.exchange = s.exchange
        INNER JOIN dwh.sources sr
            ON sr.source_name = s.source
    ;

    GET DIAGNOSTICS v_row_cnt = ROW_COUNT;

    PERFORM dwh.p_execution_log(
        v_execution_id,
        'dwh.load_purchases_history',
        'END',
        v_row_cnt
    );
     -- close connection
    PERFORM dwh.dblink_disconnect('log_conn');

    EXCEPTION WHEN OTHERS THEN

    -- ERROR log
    PERFORM dwh.p_execution_log(
        v_execution_id,
        'dwh.load_purchases_history',
        'ERROR',
        NULL,
        NULL,
        SQLERRM
    );

    PERFORM dwh.dblink_disconnect('log_conn');

    RAISE;

END;
$$;