#!/usr/bin/env python3

# Simple data connector. This data connector does not superseeds the data
# connector previously written. This data connector will be run on a local
# machine on the Oyu Tolgoi site. It will simply get data around manually
# processed events. This script will be scheduled to run every few minutes
# and will send data both the the seismic processing platform.

from microquake.core import Stream, UTCDateTime, read_events, read
# from microquake.IMS import web_client
from spp.utils.application import Application
import os
import numpy as np
from glob import glob
import redis

__module_name__ = 'initializer'

app = Application(module_name=__module_name__)
app.init_module()

consumer = app.get_kafka_consumer(logger=app.logger)
consumer.subscribe([app.settings.initializer.kafka_consumer_topic])

while True:
    msg_in = app.consumer.poll(timeout=1)
    if msg_in is None:
        continue
    if msg_in.value() == b'Broker: No more messages':
        continue

    redis_key = msg_in
    logger.info(msg_in)
    # mseed_bytes = app.redis_conn.get(redis_key)





# site = app.get_stations()
#
#
#
# black_list = ['23', '31', '32', '100', '102', '107', '88', '90', '77']
#
# base_dir = app.settings.data_connector.path
#
# api_base_url = app.settings.seismic_api.base_url
#
# for input_file in glob(os.path.join(base_dir, '*20s.mseed')):
#
#     # getting the event data to the event database endpoint
#     app.logger.info('reading files')
#     # input_file = os.path.join(base_dir, '2018_11_23T11_41_03.347319Z.xml')
#     event_file = input_file.replace('_20s.mseed', '.xml')
#     mseed_file = input_file.replace('_20s.mseed', '.mseed')
#     cmseed_file = input_file
#
#     cat = read_events(event_file)
#     app.logger.info(cat[0].preferred_origin().loc)
#     st = read(mseed_file)
#     st_c = read(cmseed_file)
#     app.logger.info('done reading files')
#
#
#     app.logger.info('preparing data')
#     event_time = cat[0].preferred_origin().time
#
#     app.logger.info('trimming stream')
#     st_c_trimmed = st_c.copy().taper(max_percentage=0.1).trim(
#                                starttime=event_time-0.2,
#                                endtime=event_time+1, pad=True,
#                                fill_value=0).taper(
#         max_percentage=0.1).filter('bandpass', freqmin=100, freqmax=1000)
#     app.logger.info('done trimming stream')
#
#
#     trs = []
#     for tr in st_c_trimmed:
#         station = site.select(station=tr.stats.station).stations()[0]
#         if station.code in black_list:
#             continue
#         if station.motion_type == 'acceleration':
#             continue
#         if np.max(tr.data) < 10 * np.std(tr.data):
#             continue
#         tr.data = np.nan_to_num(tr.data)
#         trs.append(tr)
#
#     st_c_trimmed = Stream(traces=trs)
#
#     app.send_message(cat, st_c_trimmed)
