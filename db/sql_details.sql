-- Creating the User for log operations
CREATE USER logger_user WITH PASSWORD 'strong_password';
GRANT CONNECT ON DATABASE postgres TO logger_user;
GRANT USAGE ON SCHEMA dwh TO logger_user;
GRANT INSERT ON dwh.execution_log TO logger_user;