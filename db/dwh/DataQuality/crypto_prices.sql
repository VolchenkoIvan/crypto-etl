DECLARE
    today_int INT := to_char(CURRENT_DATE, 'YYYYMMDD')::int;
    today_minus_24 INT := to_char(CURRENT_DATE - 24, 'YYYYMMDD')::int;

if 1=1:
    DECLARE
        -- Переменные коэф. аномалий
        low_ratio: FLOAT = 0.5;
        high_ratio: FLOAT = 1.5;
        current_cnt FLOAT;
        avg_hist_cnt FLOAT;

    WITH hist AS (
        SELECT COUNT(*) AS cnt
        FROM dwh.crypto_prices
        WHERE date_id < today_int
            and date_id >= today_minus_24
        GROUP BY date_id
    ),
    cur AS (
        SELECT COUNT(*) AS cnt
        FROM dwh.crypto_prices
        WHERE date_id = today_int
    )
    SELECT
        current_cnt = COALESCE((SELECT cnt FROM cur), 0),
        avg_hist_cnt = COALESCE((SELECT AVG(cnt) FROM hist), 0)