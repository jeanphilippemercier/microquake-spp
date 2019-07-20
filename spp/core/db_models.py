import sqlalchemy as db
metadata = db.MetaData()
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column, Integer, String

processing_logs = db.Table('processing_logs', metadata,
                            db.Column('event_id',db.String(255),
                                      primary_key=True),
                            db.Column('event_timestamp', db.DateTime),
                            db.Column('processing_timestamp', db.DateTime),
                            db.Column('processing_step_name', db.String(255)),
                            db.Column('processing_step_id', db.Integer),
                            db.Column('processing_delay_second', db.Float),
                            db.Column('processing_time_second', db.Float),
                            db.Column('processing_status', db.String(255)))

processing = db.Table('processing', metadata,
                      db.Column('event_id', db.String(255), primary_key=True),
                      db.Column('P_picks', db.Integer),
                      db.Column('S_picks', db.Integer),
                      db.Column('processing_delay_second', db.Float),
                      db.Column('processing_completed_timestamp',
                                db.Float),
                      db.Column('event_category', db.String(255)),
                      db.Column('event_status', db.String(255)))

# class data_quality(Base):
#     __tablename__ = 'data_quality'
#
#     id = Column(db.Integer, primary_key=True)
#     time = Column(db.DateTime, nullable=False)
#     processing_time = Column(db.DateTime, nullable=True)
#     sensor_id = Column(db.Integer, nullable=False)
#     data_quality_index = Column
# data_quality = db.Table('data_quality', metadata,
#                         db.Column('timestamp', db.DateTime),
#                         db.Column('processing_timestamp', db.DateTime),
#                         db.Column('data_quality_index', db.Float),
#                         db.Column('station', db.String(8)),
#                         db.Column('location', db.String(8)),
#                         db.Column('component', db.String(3)),
#                         db.Column('percent_recovery', db.Float),
#                         db.Column('signal_std', db.Float),
#                         db.Column('signal_energy', db.Float),
#                         db.Column('signal_max_amplitude', db.Float),
#                         db.Column('signal_dominant_frequency', db.Float),
#                         db.Column('average_cross_correlation', db.Float))


# recordings = db.Table('recordings', metadata,
#                      Column('id', db.Integer, primary_key=True),
#                      Column('time', db.DateTime),
#                      Column('sensor_id', db.Integer),
#                      Column('sample_count', db.Integer),
#                      Column('sample_rate', db.Float)
#                      # Column('x', db.(db.Float), nullable=False),
#                      # Column('y', db.ARRAY(db.Float), nullable=False),
#                      # Column('z', db.ARRAY(db.Float), nullable=False)
#                      )


class Recording(Base):
    __tablename__ = 'recordings'

    id = Column(db.Integer, primary_key=True)
    starttime = Column(db.DateTime, nullable=False)
    endtime = Column(db.DateTime, nullable=False)
    sensor_id = Column(db.Integer, nullable=False)
    sample_count = Column(db.Integer, nullable=False)
    sample_rate = Column(db.Float, nullable=False)
    # x = Column(db.ARRAY(db.Float), nullable=False)
    # y = Column(db.ARRAY(db.Float), nullable=False)
    # z = Column(db.ARRAY(db.Float), nullable=False)
    data = Column(db.LargeBinary)
    # data = Column(db.String(255))


# Base.metadata.create_all(engine)

# CREATE database sensor_data;
# CREATE USER sensor_user WITH ENCRYPTED PASSWORD 'rs12zGgdMMH1';
# GRANT ALL PRIVILEGES ON DATABASE sensor_data TO sensor_user;
# \c sensor_data
# CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
# CREATE TABLE recordings (
#   time            TIMESTAMPTZ       NOT NULL,
#   sensor_id       INT               NOT NULL,
#   sensor_type_id  INT               NOT NULL,
#   sample_count    INT               NOT NULL,
#   sample_rate     DOUBLE PRECISION  NOT NULL,
#   x               REAL ARRAY        NULL,
#   y               REAL ARRAY        NULL,
#   z               REAL ARRAY        NULL
# );
# SELECT create_hypertable('recordings', 'time');
# ALTER TABLE recordings OWNER TO sensor_user;

