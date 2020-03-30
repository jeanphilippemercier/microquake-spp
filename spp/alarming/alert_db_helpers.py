import sqlalchemy as db
metadata = db.MetaData()
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import (TIMESTAMP, INTEGER, VARCHAR,
                                            BOOLEAN)
from microquake.core.settings import settings
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError


class AlarmingState(Base):

    __tablename__ = 'alarms'

    time = Column(TIMESTAMP(timezone=True), primary_key=True)
    alert_type = Column(VARCHAR(128))
    alert_level = Column(INTEGER())  # Alarm level are represented by
    # integer from 0 to 3. Alarm level 0 means there is no alarm, alarm 3
    # means the highest level of alarm
    alert_sent = Column(BOOLEAN)


def connect_postgres():

    db_name = settings.ALERT_POSTGRES_DB_NAME
    postgres_url = settings.ALERT_POSTGRES_URL + db_name

    engine = db.create_engine(postgres_url, poolclass=NullPool,
                              connect_args={'connect_timeout': 10})

    if not database_exists(engine.url):
        create_database(engine.url)

    connection = engine.connect()
    # Create tables if they do not exist

    AlarmingState.metadata.create_all(engine)
    # metadata.create_all(engine)

    return connection, engine


def create_postgres_session():

    db_name = settings.ALERT_POSTGRES_DB_NAME
    postgres_url = settings.ALERT_POSTGRES_URL + db_name

    engine = db.create_engine(postgres_url, poolclass=NullPool,
                              connect_args={'connect_timeout': 10})

    try:
        pg = connect_postgres()
    except OperationalError:
        engine = db.create_engine(settings.ALERT_POSTGRES_URL,
                                  poolclass=NullPool,
                                  connect_args={'connect_timeout': 10})
        engine.execute(f"CREATE DATABASE {db_name}")
        engine.execute(f"USE {db_name}")

    session = sessionmaker(bind=engine)

    return session(), engine