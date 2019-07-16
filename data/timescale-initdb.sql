CREATE database sensor_data;
CREATE USER sensor_user WITH ENCRYPTED PASSWORD 'rs12zGgdMMH1';
GRANT ALL PRIVILEGES ON DATABASE sensor_data TO sensor_user;
\c sensor_data
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE TABLE recordings (
  time        TIMESTAMPTZ        NOT NULL,
  sensor_id   INT                NOT NULL,
  samplecount INT                NOT NULL,
  samplerate  INT                NOT NULL,
  waveform    DOUBLE PRECISION ARRAY NULL
);
SELECT create_hypertable('recordings', 'time');

