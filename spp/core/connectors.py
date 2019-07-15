from redis import Redis
from spp.core.settings import settings
import sqlalchemy as db
from datetime import datetime
from pytz import utc
from spp.stats_collector.db_models import processing_logs
from spp.stats_collector import db_models


def connect_redis():
    if 'REDIS_MASTER_SERVICE_HOST' in settings:
        redis_db = dict(
            host=settings.REDIS_MASTER_SERVICE_HOST,
            port=settings.REDIS_MASTER_SERVICE_PORT,
            password=settings.REDIS_PASSWORD
        )
    else:
        redis_db = settings.get('redis_db')

    redis_config = redis_db

    return Redis(**redis_config)


def connect_postgres(db_name='spp'):

    if 'POSTGRES_MASTER_SERVICE_HOST' in settings:
        pass
        # postgres_config = dict(host=settings.POSTGRES_MASTER_SERVICE_HOST,
        #                        port=settings.POSTGRES_MASTER_SERVICE_PORT,
        #                        user=settings.POSTGRES_MASTER_SERVICE_USER,
        #                        password=settings.POSTGRES_MASTER_SERVICE_PASSWORD)

    else:
        postgres_url = settings.get('postgres_db').url + db_name

    engine = db.create_engine(postgres_url)
    connection = engine.connect()
    # Create tables if they do not exist
    db_models.metadata.create_all(engine)

    return connection


def record_processing_logs_pg(event, status, processing_step,
                              processing_step_id, processing_time_second,
                              db_name='spp'):

    """
    Record the processing logs in the postgres database
    :param event: event being processed
    :param status: processing status (accepted values are success, failed)
    :param processing_step: processing step name
    :param processing_step_id: processing step identifier integer
    :param processing_time_second: processing dealy for this step in seconds
    :param processing_time_second: processing time for this step in seconds
    :param db_name: database name
    :return:
    """


    event_time = event.preferred_origin().time.datetime.replace(tzinfo=utc)

    processing_time = datetime.utcnow().replace(tzinfo=utc)
    processing_delay_second = (processing_time - event_time).total_seconds()

    document = {'event_id'               : event.resource_id.id,
                'event_timestamp'        : event_time,
                'processing_timestamp'   : processing_time,
                'processing_step_name'   : processing_step,
                'processing_step_id'     : processing_step_id,
                'processing_delay_second': processing_delay_second,
                'processing_time_second' : processing_time_second,
                'processing_status'      : status}

    with connect_postgres(db_name=db_name) as pg:
        query = db.insert(processing_logs)
        values_list = [document]

        result = pg.execute(query, values_list)

    return result


def record_processing(event):

    processing_delay_second = datetime.utcnow().timestamp() - \
            event.preferred_origin().time.datetime.timestamp()

    processing_complete_timestamp = datetime.utcnow().replace(tzinfo=utc)

    if event.evaluation_status == 'rejected':
        p_picks = 0
        s_picks = 0
        event_category = 'noise'

    elif len(event.picks) == 0:
        p_picks = 0
        s_picks = 0
        event_category = event.event_type

    else:

        len([])



    p_picks = event.preferred_arrival
    processing = db.Table('processing', metadata,
                          db.Column('event_id', db.String(255)),
                          db.Column('P_picks', db.Integer),
                          db.Column('S_picks', db.Integer),
                          db.Column('processing_delay_second', db.Float),
                          db.Column('processing_completed_timestamp',
                                    db.Float),
                          db.Column('event_category', db.String(255)),
                          db.Column('event_status', db.String(255)))


