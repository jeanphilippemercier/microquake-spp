"""
Reconciliate the IMS database and the Microquake database
All accepted events should be in the Microquake database
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
from spp.core.connectors import connect_redis, connect_mongo

# settings
redis = connect_redis()
mongo_client = connect_mongo()
processed_events_db = settings.get('mongo_db').db_processed_events
db = mongo_client[processed_events_db]
collection = db['event_status']

tz = get_time_zone()
sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')

reconciliation_delay_minutes = settings.get(
    'data_connector').reconciliation_delay_minutes

reconciliation_interval_hours = settings.get(
    'data_connector').reconciliation_interval_hours

while 1:

    # time in UTC
    endtime = datetime.now() - timedelta(minutes=reconciliation_delay_minutes)
    starttime = endtime - timedelta(hours=reconciliation_interval_hours)

    if collection.count_documents({}):
        last = collection.find_one(sort=[({'timestamp', -1})])['timestamp']
        starttime = datetime.fromtimestamp(last).replace(
            tzinfo=utc).astimezone(tz) + timedelta(seconds=1)
    else:
        starttime = endtime - timedelta(hours=48)

    try:
        cat = web_client.get_catalogue(base_url, starttime, endtime, sites, tz,
                                       accepted=True, manual=True)
    except ConnectionError:
        logger.error('Connection to the IMS server on {} failed!'.format(
            base_url))
        sleep(30)

        continue

    logger.info('recovered {} events'.format(len(cat)))

    if len(cat) == 0:
        sleep(10)

        continue

    ct = 0

    for event in cat:
        event_id = event.resource_id.id

        if collection.count_documents({'event_id': event_id}) == 0:
            ct += 1
            logger.info('Recording events to Mongo')
            insert_mongo(event)
            logger.info('sending events to redis the {} message_queue'.format(
                message_queue))
            send_to_redis(event)

    logger.info('sent {} events for further processing'.format(ct))

    # input('done')

