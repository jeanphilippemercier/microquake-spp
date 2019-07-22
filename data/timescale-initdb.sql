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
  sample_rate     DOUBLE PRECISION  NOT NULL,
  x               DOUBLE PRECISION ARRAY NULL,
  y               DOUBLE PRECISION ARRAY NULL,
  z               DOUBLE PRECISION ARRAY NULL
);
SELECT create_hypertable('recordings', 'time', chunk_time_interval => interval '1 hour');
ALTER TABLE recordings OWNER TO sensor_user;
CREATE INDEX idx_sensor_data_end_time ON recordings(end_time);
CREATE INDEX idx_sensor_data_sensor_id ON recordings(sensor_id);
SELECT drop_chunks(interval '5 hours', 'recordings');
