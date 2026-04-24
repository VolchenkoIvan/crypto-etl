CREATE OR REPLACE FUNCTION dwh.p_execution_log(
    p_procedure_name TEXT,
    p_status         TEXT,              -- 'START' | 'END' | 'ERROR'
    p_params         TEXT DEFAULT NULL,
    p_error_message  TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO dwh.execution_log (
        procedure_name,
        status,
        params,
        error_message
    )
    VALUES (
        p_procedure_name,
        UPPER(p_status),
        p_params,
        p_error_message

    );
END;
$$;
