CREATE OR REPLACE FUNCTION dwh.p_execution_log(
    p_execution_id UUID,
    p_procedure_name TEXT,
    p_status TEXT,
    p_row_cnt INT DEFAULT NULL,
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
        'INSERT INTO dwh.execution_log
        (execution_id, procedure_name, status, row_cnt, params, error_message)
         VALUES (%L, %L, %L, %s, %L, %L)',
        p_execution_id,
        p_procedure_name,
        p_status,
        COALESCE(p_row_cnt::TEXT, 'NULL'),
        p_params,
        p_error_message
    );

    PERFORM dwh.dblink_exec('log_conn', v_sql);

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Logging failed: %', SQLERRM;
END;
$$;