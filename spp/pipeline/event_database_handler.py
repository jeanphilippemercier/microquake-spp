from io import BytesIO
from time import time

from microquake.io import msgpack
from spp.utils import seismic_client
from spp.utils.seismic_client import post_data_from_objects


def process(cat=None, stream=None, logger=None, app=None, module_settings=None, prepared_objects=None):
    api_base_url = app.settings.seismic_api.base_url
    logger.info('posting data to the API')
    result = post_data_from_objects(api_base_url, event_id=None, event=cat,
                                    stream=stream, context_stream=None, tolerance=None)
    logger.info('posting seismic data')

    if result.status_code == 200:
        logger.info('successfully posting data to the API')
    else:
        logger.error('Error in postion data to the API. Returned with '
                     'error code %d' % result.status_code)

    return cat, stream
