import sqlalchemy as db
metadata = db.MetaData()
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import (TIMESTAMP, INTEGER,
                                            DOUBLE_PRECISION, VARCHAR,
                                            FLOAT)

processing_logs = db.Table('processing_logs', metadata,
                           db.Column('id', db.Integer, primary_key=True),
                           db.Column('event_id', db.String(255)),
                           db.Column('event_timestamp', db.DateTime),
                           db.Column('processing_timestamp', db.DateTime),
                           db.Column('processing_step_name', db.String(255)),
                           db.Column('processing_step_id', db.Integer),
                           db.Column('processing_delay_second', db.Float(
                               precision=8)),
                           db.Column('processing_time_second', db.Float(
                               precision=8)),
                           db.Column('processing_status', db.String(255)))

processing = db.Table('processing', metadata,
                      db.Column('event_id', db.String(255), primary_key=True),
                      db.Column('P_picks', db.Integer),
                      db.Column('S_picks', db.Integer),
                      db.Column('processing_delay_second', db.Float),
                      db.Column('processing_completed_timestamp',
                                db.Float),
                      db.Column('event_category', db.String(255)),
                      db.Column('event_status', db.String(255)),
                      db.Column('event_magnitude', db.Integer))


# class to store the ground velocity measure from the continous ground
# vibration. The main purpose is for rapid alarming.

ground_velocity = db.Table('ground_velocity', metadata,
                           db.Column('time', db.DateTime(timezone=True),
                                     primary_key=True),
                           db.Column('sensor_id', db.Integer),
                           db.Column('ground_velocity_mm_s', db.Float))

Base = declarative_base()


# class Recordings(Base):
#     __tablename__ = 'record'


class ContinuousData(Base):

    """
    The continuous data are to be stored in Redis, Postgresql database is
    used to enable the retrieval of continuous data
    """

    __tablename__ = 'continuous_data'

    """
    All time are to be stored in UTC time not in the local timezone
    """

    id = Column(INTEGER, autoincrement=True, primary_key=True)
    start_time = Column(TIMESTAMP(timezone=True), index=True)
    end_time = Column(TIMESTAMP(timezone=True), index=True)
    sensor_id = Column(VARCHAR(5), index=True)
    expiry_time = Column(TIMESTAMP(timezone=True))
    redis_key = Column(VARCHAR(40))


class Trigger(Base):

    """
    Probably to eventually be moved to the API or an API
    """

    __tablename__ = 'trigger'

    id = Column(INTEGER, autoincrement=True, primary_key=True)
    trigger_on_time = Column(TIMESTAMP(timezone=True), index=True)
    trigger_off_time = Column(TIMESTAMP(timezone=True), index=True)
    trigger_amplitude = Column(DOUBLE_PRECISION)
    sensor_id = Column(VARCHAR(5), index=True)
    trigger_band_id = Column(VARCHAR(25))

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




