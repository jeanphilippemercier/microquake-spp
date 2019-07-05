"""
Retrieve the catalog in a scheduled fashion, then send the individual
events packaged as cataglog to a Redis Queue
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
from spp.core.settings import settings
from spp.core.time import get_time_zone
from microquake.IMS import web_client
from pytz import utc
from io import BytesIO
from redis import Redis
import msgpack
from loguru import logger
from time import sleep

client = MongoClient()

#### settings
mongo_url = settings.get('mongo_db').url
mongo_client = MongoClient(mongo_url)
processed_events_db = settings.get('mongo_db').db_processed_events
db = mongo_client[processed_events_db]
collection = db['processed_events']

tz = get_time_zone()
sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('data_connector').url

redis_config = settings.get('redis_db')
redis = Redis(**redis_config)
message_queue = settings.get('processing_flow').extract_waveforms.message_queue


def insert_mongo(event):

    ev_time = event.preferred_origin().time
    record = {'timestamp': ev_time.datetime.timestamp(),
              'event_id': event.resource_id.id}

    collection.insert_one(record)

def send_to_redis(event):
    file_out = BytesIO()
    event.write(file_out, format='quakeml')

    dict_out = {'event_bytes': file_out.getvalue(),
                'processing_attempts': 0}

    msg = msgpack.dumps(dict_out)
    redis.rpush(message_queue, msg)


###
# REMOVE AT ALL COST WHEN DEVELOPMENT IS COMPLETED!!!
###

collection.drop()

while 1:

    # time in UTC
    endtime = datetime.now()
    if collection.count_documents({}):
        last = collection.find_one(sort=[({'timestamp', -1})])['timestamp']
        starttime = datetime.fromtimestamp(last).replace(
            tzinfo=utc).astimezone(tz) + timedelta(seconds=1)
    else:
        starttime = endtime - timedelta(hours=48)

    cat = web_client.get_catalogue(base_url, starttime, endtime, sites, tz,
                                   accepted=True, manual=True)
                                   # accepted=False, manual=False)

    logger.info('recovered {} events'.format(len(cat)))

    if len(cat) == 0:
        sleep(2)
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


