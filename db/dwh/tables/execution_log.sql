CREATE TABLE IF NOT EXISTS dwh.execution_log (
    procedure_name  TEXT        NOT NULL,
    status          TEXT        NOT NULL, -- START / END / ERROR
    params          TEXT        NULL,
    error_message   TEXT        NULL,
    created_at      TIMESTAMP   DEFAULT NOW()
);

-- быстрый поиск по процедуре и времени
CREATE INDEX idx_proc_log_name
ON dwh.execution_log (procedure_name);

CREATE INDEX idx_proc_log_time
ON dwh.execution_log (created_at DESC);