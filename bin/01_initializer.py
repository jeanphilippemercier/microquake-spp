#!/usr/bin/env python3

# Simple data connector. This data connector does not superseeds the data
# connector previously written. This data connector will be run on a local
# machine on the Oyu Tolgoi site. It will simply get data around manually
# processed events. This script will be scheduled to run every few minutes
# and will send data both the the seismic processing platform.

from spp.utils.application import Application
from spp.utils import seismic_client
from io import BytesIO
from microquake.core import read
import json

__module_name__ = 'initializer'

app = Application(module_name=__module_name__)
app.init_module()
redis_conn = app.init_redis()
logger = app.logger
api_base_url = app.settings.seismic_api.base_url
params = app.settings.initializer
site = app.get_stations()

consumer = app.get_kafka_consumer(logger=app.logger)
consumer.subscribe([app.settings.initializer.kafka_consumer_topic])

logger.info('awaiting for messages on channe %s'
            % app.settings.initializer.kafka_consumer_topic)
while True:
    msg_in = app.consumer.poll(timeout=1)
    if msg_in is None:
        continue
    if msg_in.value() == b'Broker: No more messages':
        continue

    msg_dict = json.loads(msg_in.value())
    logger.info(msg_in)
    redis_key = msg_dict['redis_key']
    event_id = msg_dict['waveform_id']

    continuous_data_io = BytesIO(redis_conn.get(redis_key))
    continuous_data = read(continuous_data_io, format='MSEED')

    request_event = seismic_client.get_event_by_id(api_base_url, event_id)
    cat = request_event.get_event()
    event_time = cat[0].preferred_origin().time

    start = params.window_size.start
    end = params.window_size.end

    for tr in continuous_data:
        if tr.stats.station in app.settings.black_list.stations:
            continue
        station = site.select(station=tr.stats.station).stations()[0]
        tr.taper(max_percentage=0.01)
        tr.trim(starttime=event_time+start, endtime=event_time+end, pad=True,
                fill_value=0)
        tr.filter('bandpass', **app.settings.initializer.filter)
        if station.motion_type == 'acceleration':
            tr.integrate()

    app.send_message(cat, continuous_data)
    logger.info('awaiting for message on channe %s'
                % app.settings.initializer.kafka_consumer_topic)
