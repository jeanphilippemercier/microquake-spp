#!/usr/bin/env python3
# This modules expect to receive a message containing the following:
# [catalog, stream, context_stream, event_id]

from io import BytesIO
from microquake.io import msgpack
from time import time
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from spp.utils import seismic_client
from spp.utils.seismic_client import (build_request_data_from_object,
                                          post_event_data)

__module_name__ = 'event_database_handler'

app = Application(module_name=__module_name__)
app.init_module()

app.logger.info('awaiting message from Kafka')
while True:
    msg_in = app.consumer.poll(timeout=1)
    if msg_in is None:
        continue
    if msg_in.value() == b'Broker: No more messages':
        continue
    cat, stream, extra_msgs = app.receive_message(msg_in)

    app.logger.info('creating request from seismic data')
    request_data, request_files = build_request_data_from_object(
        event_id=None, event=cat, stream=stream, context_stream=None)
    app.logger.info('done creating request from seismic data')

    api_base_url = app.settings.seismic_api.base_url

    app.logger.info('posting data to the API')
    result = post_event_data(api_base_url, request_data, request_files)
    if result.code == 200:
        app.logger.info('successfully posting data to the API')
    else:
        app.logger.error('Error in postion data to the API. Returned with '
                         'error code %d' % result.code)
