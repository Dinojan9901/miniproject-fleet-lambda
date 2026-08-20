-- Airflow keeps its metadata in its own database on the same server.
-- Separate database, not a separate schema: Airflow runs its own migrations and
-- should never be able to touch the pipeline's tables.
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO fleet;
