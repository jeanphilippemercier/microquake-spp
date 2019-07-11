"""
Retrieve the catalog in a scheduled fashion, then send the individual
events packaged as cataglog to a Redis Queue
"""

from datetime import datetime, timedelta
from io import BytesIO
from time import sleep

import msgpack
from pytz import utc

from loguru import logger
from microquake.IMS import web_client
from spp.core.settings import settings
from spp.core.time import get_time_zone
from spp.core.connectors import (connect_redis, connect_postgres)

from spp.core.db_models import processing_logs
import sqlalchemy as db

# settings
redis = connect_redis()
request_range_hours = settings.get('data_connector').request_range_hours

pg = connect_postgres(db_name='spp')

# elastic_index = 'spp'
# elastic_doc_type = 'processing_log'
#

tz = get_time_zone()
sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')

message_queue = settings.get('processing_flow').extract_waveforms.message_queue

# def insert_elastic(event, status):
#     event_timestamp = event.preferred_origin().time.datetime.timestamp()
#     processing_completion_timestamp = datetime.utcnow().timestamp()
#     processing_time_seconds = processing_completion_timestamp - event_timestamp
#
#
#     document = {'event_id': event.resource_id.id,
#                 'event_timestamp': event_timestamp,
#                 'processing_timestamp': datetime.utcnow().timestamp(),
#                 'processing_step_name': 'initialization',
#                 'processing_step_id': 1,
#                 'processing_time_seconds': processing_time_seconds,
#                 'status': status}
#
#     es.index(index=elastic_index, doc_type=elastic_doc_type, body=document)


def insert_postgres(event, status):
    event_time = event.preferred_origin().time.datetime.replace(tzinfo=utc)

    processing_time = datetime.utcnow().replace(tzinfo=utc)
    processing_time_seconds = processing_time - event_time

    # from pdb import set_trace;set_trace()

    document = {'event_id'               : event.resource_id.id,
                'event_timestamp'        : event_time,
                'processing_timestamp'   : processing_time,
                'processing_step_name'   : 'initialization',
                'processing_step_id'     : 1,
                'processing_time_seconds': processing_time_seconds.total_seconds(),
                'processing_status'      : status}

    query = db.insert(processing_logs)
    values_list = [document]

    return pg.execute(query, values_list)


def get_starttime():

    query = db.select([db.func.max(
        processing_logs.columns.event_timestamp)]).where(
        processing_logs.columns.processing_step_name=='initialization')

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


def send_to_redis(event):
    file_out = BytesIO()
    event.write(file_out, format='quakeml')

    dict_out = {'event_bytes': file_out.getvalue(),
                'processing_attempts': 0}

    msg = msgpack.dumps(dict_out)
    redis.rpush(message_queue, msg)

while 1:

    # time in UTC
    endtime = datetime.utcnow().replace(tzinfo=utc)
    starttime = get_starttime()

    try:
        cat = web_client.get_catalogue(base_url, starttime, endtime, sites,
                                       utc, accepted=False, manual=False)
    except ConnectionError:
        logger.error('Connection to the IMS server on {} failed!'.format(
            base_url))
        sleep(30)
        continue

    # from pdb import set_trace; set_trace()

    logger.info('recovered {} events'.format(len(cat)))

    if len(cat) == 0:
        sleep(10)

        continue

    ct = 0

    sorted_cat = sorted(cat, reverse=True,
                        key=lambda x: x.preferred_origin().time)

    for event in sorted_cat:
        event_id = event.resource_id.id

        if not already_processed(event):
            ct += 1
            logger.info('sending events with event_id {} to redis the {} '
                        'message_queue'.format(event.resource_id.id,
                message_queue))
            try:
                send_to_redis(event)
                logger.info('sent {} events for further processing'.format(ct))
                status = 'success'
            except:
                logger.error('Sending to redis failed')
                status = 'failed'

            # insert_elastic(event, status=status)
            insert_postgres(event, status=status)

    # input('done')
