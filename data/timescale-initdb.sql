CREATE database sensor_data;
CREATE USER sensor_user WITH ENCRYPTED PASSWORD 'rs12zGgdMMH1';
GRANT ALL PRIVILEGES ON DATABASE sensor_data TO sensor_user;
\c sensor_data
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE TABLE recordings (
  time            TIMESTAMPTZ       NOT NULL,
  end_time        TIMESTAMPTZ       NULL,
  sensor_id       INT               NOT NULL,
  sensor_type_id  INT               NULL,
  sample_count    INT               NOT NULL,
  sample_rate     REAL              NOT NULL,
  x               REAL ARRAY        NULL,
  y               REAL ARRAY        NULL,
  z               REAL ARRAY        NULL
);
SELECT create_hypertable('recordings', 'time');
ALTER TABLE recordings OWNER TO sensor_user;
