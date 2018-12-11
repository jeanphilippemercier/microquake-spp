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

def event_database_handler(cat=None, stream=None, logger=None):
    logger.info('creating request from seismic data')
    request_data, request_files = build_request_data_from_object(
        event_id=None, event=cat, stream=stream, context_stream=None)
    logger.info('done creating request from seismic data')

    api_base_url = app.settings.seismic_api.base_url

    logger.info('posting data to the API')
    result = post_event_data(api_base_url, request_data, request_files)
    if result.status_code == 200:
        logger.info('successfully posting data to the API')
    else:
        logger.error('Error in postion data to the API. Returned with '
                     'error code %d' % result.status_code)

    return cat, stream


__module_name__ = 'event_database_handler'

app = Application(module_name=__module_name__)
app.init_module()

app.logger.info('awaiting message from Kafka')

try:
    while True:
        msg_in = app.consumer.poll(timeout=1)
        if msg_in is None:
            continue
        if msg_in.value() == b'Broker: No more messages':
            continue

        try:
            cat, stream = app.receive_message(msg_in, event_database_handler)
        except Exception as e:
            app.logger.error(e)

except KeyboardInterrupt:
    pass

finally:
    app.consumer.close()

