-- Расширение для параллельного открытия сессии (для логов)
CREATE EXTENSION dblink;
-- Расширение для автоматической генерации UUID (для логов)
CREATE EXTENSION IF NOT EXISTS pgcrypto;