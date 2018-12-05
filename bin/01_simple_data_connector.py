#!/usr/bin/env python3

# Simple data connector. This data connector does not superseeds the data
# connector previously written. This data connector will be run on a local
# machine on the Oyu Tolgoi site. It will simply get data around manually
# processed events. This script will be scheduled to run every few minutes
# and will send data both the the seismic processing platform.

from microquake.core import Stream, UTCDateTime, read_events, read
# from microquake.IMS import web_client
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from glob import glob
from microquake.io import msgpack
from spp.utils import seismic_client
from io import BytesIO
import os
import numpy as np
import uuid

 # looking at the past 10 hours

app = Application()
params = app.settings.data_connector
site = app.get_stations()
tz = app.get_time_zone()
settings = app.settings

logger = app.get_logger(settings.data_connector.log_topic,
                        settings.data_connector.log_file_name)

logger.info('setting up Kafka')
k_producer = app.get_kafka_producer(logger=logger)
k_consumer = app.get_kafka_consumer(logger=logger)
k_consumer.subscribe([settings.data_connector.kafka_consumer_topic])
logger.info('done setting up Kafka')

black_list = ['23', '31', '32', '100', '102', '107', '88', '90', '77']

base_dir = params.path

api_base_url = app.settings.seismic_api.base_url

# for input_file in glob(os.path.join(base_dir, '*20s.xml')):

    # getting the event data to the event database endpoint
logger.info('reading files')
input_file = os.path.join(base_dir, '2018_11_23T11_41_03.347319Z.xml')
event_file = input_file
mseed_file = input_file.replace('xml', 'mseed')
cmseed_file = input_file.replace('.xml', '_20s.mseed')

cat = read_events(event_file)
logger.info(cat[0].preferred_origin().loc)
st = read(mseed_file)
st_c = read(cmseed_file)
logger.info('done reading files')

logger.info('init connection to redis')
redis_conn = app.init_redis()
logger.info('connectiong to redis database successfully initated')

logger.info('preparing data')
event_time = cat[0].preferred_origin().time

logger.info('trimming stream')
st_c_trimmed = st_c.copy().taper(max_percentage=0.1).trim(
                           starttime=event_time-0.2,
                           endtime=event_time+1, pad=True,
                           fill_value=0).taper(
    max_percentage=0.1).filter('bandpass', freqmin=100, freqmax=1000)
logger.info('done trimming stream')


trs = []
for tr in st_c_trimmed:
    station = site.select(station=tr.stats.station).stations()[0]
    if station.code in black_list:
        continue
    if station.motion_type == 'acceleration':
        continue
    if np.max(tr.data) < 10 * np.std(tr.data):
        continue
    tr.data = np.nan_to_num(tr.data)
    trs.append(tr)

st_c_trimmed = Stream(traces=trs)

st_io = BytesIO()
st_c_trimmed.write(st_io, format='mseed')
ev_io = BytesIO()
cat.write(ev_io, format='QUAKEML')

data_out = msgpack.pack([ev_io.getvalue(), st_c_trimmed])
timestamp_ms = int(cat[0].preferred_origin().time.timestamp * 1e3)
redis_key = str(uuid.uuid4())
logger.info('done preparing data')

logger.info('sending the data to Redis')
redis_conn.set(redis_key, data_out, ex=settings.redis_extra.ttl)
logger.info('done sending the data to redis')

logger.info('sending message to kafka')

k_producer.produce(app.settings.data_connector.kafka_producer_topic,
                   redis_key)

logger.info('done sending message to kafka')