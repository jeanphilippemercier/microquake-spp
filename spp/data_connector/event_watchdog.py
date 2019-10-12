"""
Retrieve the catalog in a scheduled fashion, then send the individual
events packaged as cataglog to a Redis Queue
"""

from datetime import datetime, timedelta
from importlib import reload
from time import sleep, time

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

from microquake.core.helpers.timescale_db import get_db_lag

from timeloop import Timeloop
tl = Timeloop()

reload(pre_processing)

__processing_step__ = 'event-watchdog'
__processing_step_id__ = 1

request_range_hours = settings.get('data_connector').request_range_hours

pg = connect_postgres()

tz = get_time_zone()
sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')

we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE
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


@tl.job(interval=timedelta(seconds=5))
def retrieve_event():

    # time in UTC

    closing_window_time_seconds = settings.get(
        'data_connector').closing_window_time_seconds

    # endtime = get_db_lag().replace(tzinfo=utc)
    endtime = datetime.utcnow().replace(tzinfo=utc) - \
              timedelta(seconds=closing_window_time_seconds)

    # lag = (datetime.utcnow().replace(tzinfo=utc) - endtime).total_seconds()

    # logger.info(f'The data in the Timescale database are lagging by '
    #             f'{lag} seconds')

    starttime = get_starttime()

    cat = web_client.get_catalogue(base_url, starttime, endtime, sites,
                                   utc, accepted=False, manual=False)
    logger.info('recovered {} events'.format(len(cat)))

    if len(cat) == 0:
        sleep(10)

        return

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

            status = 'success'

            end_processing_time = time()
            processing_time = end_processing_time - start_processing_time
            result = record_processing_logs_pg(event, status,
                                               __processing_step__,
                                               __processing_step_id__,
                                               processing_time)

        logger.info('sent {} events for further processing'.format(ct))


tl.start(block=True)

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        tl.stop()
        break
