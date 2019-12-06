"""
Retrieve the catalog in a scheduled fashion, then send the individual
events packaged as cataglog to a Redis Queue
"""

from datetime import datetime, timedelta
from importlib import reload
from time import sleep, time

import signal

import sqlalchemy as db
from pytz import utc

from loguru import logger
from microquake.clients.ims import web_client
from microquake.core.helpers.time import get_time_zone
from microquake.core.settings import settings
from microquake.db.connectors import (RedisQueue, connect_postgres,
                                      record_processing_logs_pg)
from microquake.db.models.alchemy import processing_logs
from microquake.db.models.redis import set_event
from spp.data_connector import pre_processing
from spp.data_connector.pre_processing import pre_process

reload(pre_processing)

__processing_step__ = 'event-watchdog'
__processing_step_id__ = 1

request_range_hours = settings.get('data_connector').request_range_hours

pg, engine = connect_postgres()

tz = get_time_zone()
sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')

we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE
we_job_queue = RedisQueue(we_message_queue)
we_job_queue_slow = RedisQueue(we_message_queue + '.low_priority')

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


def already_processed(seismic_event):
    query = db.select([db.func.count(processing_logs.columns.event_id)]).where(
        processing_logs.columns.event_id == seismic_event.resource_id.id)

    return bool(pg.execute(query).scalar())


class TimeOutException(Exception):
    def __init__(self, message, errors):
        super(TimeOutException, self).__init__(message)
        self.errors = errors


def timeout_handler(signum, frame):
    logger.error('process has timed out')
    raise TimeOutException("Timeout", signum)


signal.signal(signal.SIGALRM, timeout_handler)

init_time = time()


def heartbeat():
    import requests
    api_base_url = settings.get('API_BASE_URL')
    if api_base_url[-1] == '/':
        api_base_url = api_base_url[:-1]
    url = api_base_url + '/inventory/heartbeat'
    return requests.post(url, json={'source': 'event_connector'})


# run for 10 minutes
while time() - init_time < 600:

    heartbeat()

    logger.info(f'Time remaining (s): {600 - (time() - init_time)}')
    # time in UTC

    closing_window_time_seconds = settings.get(
        'data_connector').closing_window_time_seconds

    endtime = datetime.utcnow().replace(tzinfo=utc) - \
              timedelta(seconds=closing_window_time_seconds)

    starttime = get_starttime()

    logger.info('retrieving catalogue from the IMS system')
    cat = web_client.get_catalogue(base_url, starttime, endtime, sites,
                                   utc, accepted=False, manual=False)

    logger.info('done retrieving catalogue')
    logger.info('recovered {} events'.format(len(cat)))

    if len(cat) == 0:
        logger.info('nothing to do ...')
        # sys.exit()
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
            set_event(event_id, catalogue=event.copy())
            
            result = we_job_queue.submit_task(pre_process, event_id=event_id)
            
            for i, offsets in enumerate([-5, -4, -3, -2, 1, 2, 3]):
                event2 = event.copy()
                event.resource_id.id += f'_{i}'
                event_id = event.resource_id.id
                event.preferred_origin().time += offset
                set_event(event_id, catalogue=event.copy())
                result2 = we_job_queue_slow.submit_task(pre_process, 
                                                        event_id=event_id)

            status = 'success'

            end_processing_time = time()
            processing_time = end_processing_time - start_processing_time

            result = record_processing_logs_pg(event, status,
                                               __processing_step__,
                                               __processing_step_id__,
                                               processing_time)

    logger.info(f'sent {ct} events for further processing')
    
pg.close()
engine.dispose()


