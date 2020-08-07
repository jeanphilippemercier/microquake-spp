from datetime import datetime, timedelta

import sqlalchemy as db
from pytz import utc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import OperationalError

from microquake.core.settings import settings
from spp.db.models.alchemy import (metadata,
                                   processing_logs,
                                   Base, triggers)
from redis import ConnectionPool, StrictRedis
from rq import Queue
from walrus import Walrus


def connect_redis():
    return RedisWrapper().redis_connect(url=settings.REDIS_WALRUS_URL)


class RedisWrapper(object):
    shared_state = {}

    def __init__(self):
        self.__dict__ = self.shared_state

    def redis_connect(self, url):
        try:
            self.connection_pool
        except AttributeError:
            self.connection_pool = ConnectionPool.from_url(url)

        return Walrus(connection_pool=self.connection_pool)


class RedisQueue:
    def __init__(self, queue, timeout=1800):
        self.redis = StrictRedis.from_url(url=settings.REDIS_RQ_URL)
        self.timeout = timeout
        self.queue = queue
        self.rq_queue = Queue(self.queue, connection=self.redis,
                              default_timeout=self.timeout)

    def submit_task(self, func, *args, **kwargs):
        return self.rq_queue.enqueue(func, *args, **kwargs)


# def submit_task_to_rq(queue, func, *args, **kwargs):
#     with connect_redis() as redis:
#         rq_queue = Queue(queue, connection=redis)
#         return rq_queue.enqueue(func, *args, **kwargs)

# rq worker --url redis://redisdb:6379 --log-format '%(asctime)s '  api

def connect_rq(message_queue):
    redis = connect_redis()

    return Queue(message_queue, connection=redis)


def connect_postgres(db_name='data'):

    # db_name = settings.POSTGRES_DB_NAME
    postgres_url = settings.POSTGRES_URL + db_name

    engine = db.create_engine(postgres_url, poolclass=NullPool,
                              connect_args={'connect_timeout': 10})

    if not database_exists(engine.url):
        create_database(engine.url)

    connection = engine.connect()
    # Create tables if they do not exist
    metadata.create_all(engine)
    Base.metadata.create_all(engine, Base.metadata.tables.values(),
                             checkfirst=True)

    return connection, engine


def connect_timescale():

    db_name = settings.TIMESCALEDB_NAME
    timescale_url = settings.TIMESCALEDB_URL + db_name

    engine = db.create_engine(timescale_url, poolclass=NullPool,
                              connect_args={'connect_timeout': 10})

    if not database_exists(engine.url):
        create_database(engine.url)

    session = sessionmaker(bind=engine)()

    return session, engine


def create_postgres_session():

    db_name = settings.POSTGRES_DB_NAME
    postgres_url = settings.POSTGRES_URL + db_name

    engine = db.create_engine(postgres_url, poolclass=NullPool,
                              connect_args={'connect_timeout': 10})

    try:
        pg = connect_postgres()
    except OperationalError:
        engine = db.create_engine(settings.POSTGRES_URL, poolclass=NullPool,
                                  connect_args={'connect_timeout': 10})
        engine.execute(f"CREATE DATABASE {db_name}")
        engine.execute(f"USE {db_name}")

    session = sessionmaker(bind=engine)

    return session(), engine, pg


def record_processing_logs_pg(event, status, processing_step,
                              processing_step_id, processing_time_second):
    """
    Record the processing logs in the postgres database
    :param event: event being processed
    :param status: processing status (accepted values are success, failed)
    :param processing_step: processing step name
    :param processing_step_id: processing step identifier integer
    :param processing_time_second: processing dealy for this step in seconds
    :return:
    """

    if 'catalog' in str(type(event)).lower():
        event = event[0]

    origin = event.preferred_origin()

    if origin is None:
        origin = event.origins[-1]

    event_time = origin.time.datetime.replace(tzinfo=utc)

    current_time = datetime.utcnow().replace(tzinfo=utc)
    processing_delay_second = (current_time - event_time).total_seconds()

    document = {'event_id': event.resource_id.id,
                'event_timestamp': event_time,
                'processing_timestamp': current_time,
                'processing_step_name': processing_step,
                'processing_step_id': processing_step_id,
                'processing_delay_second': processing_delay_second,
                'processing_time_second': processing_time_second,
                'processing_status': status}

    pg, engine = connect_postgres()
    query = db.insert(processing_logs)
    values_list = [document]
    result = pg.execute(query, values_list)

    pg.close()
    engine.dispose()

    return result


def write_trigger(trigger_on, trigger_off, amplitude, sensor_id,
                  trigger_band_id):

    document = {'trigger_on_time': trigger_on,
                'trigger_off_time': trigger_off,
                'amplitude': amplitude,
                'sensor_id': sensor_id,
                'trigger_band_id': trigger_band_id}

    return write_document_postgres(document, triggers)


def write_document_postgres(document, table):
    pg, engine = connect_postgres()
    query = db.insert(table)
    values_list = [document]
    result = pg.execute(query, values_list)

    pg.close()
    engine.dispose()

    return result
