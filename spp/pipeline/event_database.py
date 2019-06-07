# This modules expect to receive a message containing the following:
# [catalog, stream, context_stream, event_id]

from loguru import logger
from spp.utils.seismic_client import post_data_from_objects

from ..core.settings import settings


class Processor():
    def __init__(self, module_name, app=None, module_type=None):
        self.__module_name = module_name
        self.params = settings.get(self.module_name)
        self.api_base_url = settings.get('seismic_api').base_url

    @property
    def module_name(self):
        return self.__module_name


    def process(
        self,
        **kwargs
    ):
        cat = kwargs["cat"]
        stream = kwargs["stream"]

        logger.info('posting data to the API')
        result = post_data_from_objects(self.api_base_url, event_id=None, event=cat,
                                        stream=stream, context_stream=None, tolerance=None)
        logger.info('posting seismic data')

        if result.status_code == 200:
            logger.info('successfully posting data to the API')
        else:
            logger.error('Error in postion data to the API. Returned with '
                         'error code %d' % result.status_code)

        return cat, stream
