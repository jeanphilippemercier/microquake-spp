#!/usr/bin/env python3
# This modules expect to receive a message containing the following:
# [catalog, stream, context_stream, event_id]

from io import BytesIO
from microquake.io import msgpack
from time import time
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from spp.utils import seismic_client
from spp.utils.seismic_client import (post_data_from_objects)
import os

def event_save_result_local(cat=None, stream=None, logger=None,
                           api_base_url=None):

    logger.info('saving results to disk')

    spp_home = os.environ['SPP_HOME']

    # origin[0] is used here to ensure that the filename is consistent
    # throughout.

    fname = str(cat[0].origins[0].time) + '.xml'

    fpath = os.path.join(spp_home, 'results', fname)

    cat.write(fpath, format='QUAKEML')
    stream.write(fpath.replace('xml', 'mseed'), format='MSEED')

    return None


__module_name__ = 'event_database_handler'

app = Application(module_name=__module_name__)
app.init_module()
api_base_url = app.settings.seismic_api.base_url

app.logger.info('awaiting message from Kafka')

try:
    for msg_in in app.consumer:
        try:
            result = app.receive_message(msg_in, event_save_result_local,
                                              api_base_url=api_base_url)
        except Exception as e:
            app.logger.error(e)

except KeyboardInterrupt:
    app.logger.info('received keyboard interrupt')

finally:
    app.logger.info('closing Kafka connection')
    app.consumer.close()
    app.logger.info('connection to Kafka closed')
