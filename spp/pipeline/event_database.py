# This modules expect to receive a message containing the following:
# [catalog, stream, context_stream, event_id]

from loguru import logger
from spp.utils.seismic_client import post_data_from_objects

from ..core.settings import settings


class Processor():
    @property
    def module_name(self):
        return "event_database"

    def initializer(self):
        self.api_base_url = settings.API_BASE_URL

    def process(
        self,
        **kwargs
    ):
        cat = None
        stream = None
        variable_length = None
        contest = None

        if 'cat' in kwargs.keys():
            cat = kwargs['cat']
        if 'fixed_length' in kwargs.keys():
            stream = kwargs['fixed_length']
        if 'variable_length' in kwargs.keys():
            variable_length = kwargs['variable_length']
        if 'context' in kwargs.keys():
            context = kwargs['context']


        logger.info('posting data to the API')

        post_data_from_objects(self.api_base_url, event_id=None, event=cat,
                               stream=stream, context_stream=context,
                               variable_length_stream=variable_length,
                               tolerance=None,
                               send_to_bus=False,
                               logger=logger)

        logger.info('posting seismic data')

        if result.status_code == 200:
            logger.info('successfully posting data to the API')
        else:
            logger.error('Error in postion data to the API. Returned with '
                         'error code %d' % result.status_code)

        self.result = kwargs
        return kwargs

    def legacy_pipeline_handler(
        self,
        msg_in,
        res
    ):
        return res['cat'], res['stream']
