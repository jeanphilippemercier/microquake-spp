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

 # looking at the past 10 hours

app = Application()
params = app.settings.data_connector
site = app.get_stations()
tz = app.get_time_zone()
settings = app.settings

logger = app.get_logger(settings.data_connector.log_topic,
                        settings.data_connector.log_file_name)

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.data_connector.kafka_consumer_topic
kafka_producer_topics = settings.data_connector.kafka_producer_topic
kafka_handler = KafkaHandler(kafka_brokers)

black_list = ['23', '31', '32', '100', '102', '107', '88', '90', '77']

base_dir = params.path

api_base_url = app.settings.seismic_api.base_url

logger.info('awaiting Kafka mseed messsages')
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

# st_c_trimmed.traces = st_c_trimmed.traces

data_out = msgpack.pack([ev_io.getvalue(), st_c_trimmed])
timestamp_ms = int(cat[0].preferred_origin().time.timestamp * 1e3)
key = str(cat[0].preferred_origin().time).encode('utf-8')
logger.info('done preparing data')

logger.info('sending the data to Redis')
redis_conn.set(key, data_out, ex=settings.redis_extra.ttl)
logger.info('done sending the data to redis')

logger.info('sending message to kafka')

kafka_handler.send_to_kafka(kafka_producer_topics, key,
                                message=key,
                                timestamp_ms=timestamp_ms)
kafka_handler.producer.flush()

logger.info('done sending message to kafka')
# data, files = seismic_client.build_request_data_from_bytes(None,
#                                                            event_file,
#                                                            mseed_file,
#                                                            None)



# cat = read_events(event_file, format='QUAKEML')
# event_id = cat[0].resource_id.id
#
# cat_bytes = open(event_file, 'rb').read()
# st_bytes = open(mseed_file, 'rb').read()
#
# data_out = msgpack.pack([event_id, cat_bytes, st_bytes, None])
#
# timestamp_ms = int(cat[0].preferred_origin().time.timestamp * 1e3)
# key = str(cat[0].preferred_origin().time).encode('utf-8')
#
# for kptopic in kafka_producer_topics:
#     kafka_handler.send_to_kafka(kptopic,
#                                 key,
#                                 message=data_out,
#                                 timestamp_ms=timestamp_ms)



# for cmseed_file in glob(os.path.join(base_dir, '*_20s.mseed')):
#     st_c = read(cmseed_file)
#     for tr in st_c:
#         tr.stats.starttime -= 8 * 3600
#     st_c.write(cmseed_file, format='MSEED')

# base_url = params.path
# topic_in = params.kafka_consumer_topic
# topic_feedback = params.kafka_feedback_topic
# topic_out = params.kafka_producer_topic
# brokers = app.settings.kafka.brokers
# kaf_handle = KafkaHandler(brokers)
# consumer = KafkaHandler.consume_from_topic(topic_in, brokers, group_id=None)
#
# logger = app.get_logger(params.log_topic,
#                         params.log_file_name)
#
# cat = web_client.get_catalogue(base_url, start_time, end_time, site, tz)
#
# stations = [station.code for station in site.stations()]
#
# logger.info('awaiting Kafka mseed messsages')
# for message in consumer:
#
# for evt in cat:
#     stime = evt.preferred_origin().time - 5
#     etime = evt.preferred_origin().time + 15
#
#     for station in stations:
#         tmp = web_client.get_continuous(base_url, stime, etime, stations)
#         st = Stream()
#         for tr in tmp:
#             if len(tr.data) == 0:
#                 continue
#             st.traces.append(tr)
#
#         st_io = BytesIO()
#         st.write(st_io, format='MSEED')
#     # send the data to the seismic-api endpoint on Azure @Hanee, could you
#     # please complete the code required to push the mseed to the server
#
#     # CODE TO SEND THE DATA TO THE END POINT HERE
#
#     # decomposed_mseed = mseed_decomposer(st)
#     #
#     # write_decomposed_mseed_to_kafka(decomposed_mseed)
