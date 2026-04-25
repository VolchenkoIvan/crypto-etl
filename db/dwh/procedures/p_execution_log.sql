CREATE OR REPLACE FUNCTION dwh.p_execution_log(
    p_procedure_name TEXT,
    p_status TEXT,
    p_params TEXT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
BEGIN

    v_sql := format(
        'INSERT INTO dwh.execution_log(procedure_name, status, params, error_message)
         VALUES (%L, %L, %L, %L)',
        p_procedure_name,
        p_status,
        p_params,
        p_error_message
    );

    PERFORM dblink_exec('log_conn', v_sql);

EXCEPTION WHEN OTHERS THEN
    -- лог не должен ломать ETL
    RAISE NOTICE 'Logging failed: %', SQLERRM;
END;
$$;