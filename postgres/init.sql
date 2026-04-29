CREATE EXTENSION IF NOT EXISTS citus;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Display TIMESTAMPTZ values with the source offset (America/Sao_Paulo, UTC-03).
ALTER DATABASE piraweather SET timezone TO 'America/Sao_Paulo';
