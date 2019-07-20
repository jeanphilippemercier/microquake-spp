"""
Retrieve the catalog in a scheduled fashion, then send the individual
events packaged as cataglog to a Redis Queue
"""

from datetime import datetime, timedelta
from time import sleep
from pytz import utc

from loguru import logger
from microquake.IMS import web_client
from spp.core.settings import settings
from spp.core.time import get_time_zone
from spp.core.connectors import (connect_postgres,
                                 record_processing_logs_pg)
from spp.core.redis_connectors import RedisQueue

from spp.core.serializers.seismic_objects import serialize

from spp.data_connector.waveform_extractor import extract_waveform

from time import time

from spp.core.db_models import processing_logs
import sqlalchemy as db

db_name = settings.get('postgres_db').db_name
__processing_step__ = 'initializer'
__processing_step_id__ = 1

request_range_hours = settings.get('data_connector').request_range_hours

pg = connect_postgres(db_name=db_name)

tz = get_time_zone()
sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')

we_message_queue = settings.WAVEFORM_EXTRACTOR_MESSAGE_QUEUE
we_job_queue = RedisQueue(we_message_queue)

def get_starttime():

    query = db.select([db.func.max(
        processing_logs.columns.event_timestamp)]).where(
        processing_logs.columns.processing_step_name == __processing_step__)

    result = pg.execute(query).scalar()
    if result is None:
        starttime = datetime.utcnow().replace(tzinfo=utc) - \
                    timedelta(hours=request_range_hours)
    else:
        starttime = result.replace(tzinfo=utc) + timedelta(
            seconds=1)

    return starttime

def already_processed(event):
    query = db.select([db.func.count(processing_logs.columns.event_id)]).where(
        processing_logs.columns.event_id == event.resource_id.id)

    return bool(pg.execute(query).scalar())


while 1:

    # time in UTC
    endtime = datetime.utcnow().replace(tzinfo=utc)
    starttime = get_starttime()

    try:
        cat = web_client.get_catalogue(base_url, starttime, endtime, sites,
                                       utc, accepted=False, manual=False)
    except:
        logger.error('Connection to the IMS server on {} failed!'.format(
            base_url))
        sleep(30)
        continue

    logger.info('recovered {} events'.format(len(cat)))

    if len(cat) == 0:
        sleep(10)

        continue

    ct = 0

    sorted_cat = sorted(cat, reverse=True,
                        key=lambda x: x.preferred_origin().time)

    for event in sorted_cat:
        start_processing_time = time()
        event_id = event.resource_id.id

        if not already_processed(event):
            ct += 1
            logger.info('sending events with event_id {} to redis the {} '
                        'message_queue'.format(event.resource_id.id,
                                               we_message_queue))
            try:
                message = serialize(catalogue=event)
                result = we_job_queue.submit_task(extract_waveform,
                                                  kwargs={'data': message,
                                                          'serialized': True})

                logger.info('sent {} events for further processing'.format(ct))
                status = 'success'
            except:
                logger.error('Sending to redis failed')
                status = 'failed'

            end_processing_time = time()
            processing_time = end_processing_time - start_processing_time
            result = record_processing_logs_pg(event, status,
                                               __processing_step__,
                                               __processing_step_id__,
                                               processing_time,
                                               db_name=db_name)

