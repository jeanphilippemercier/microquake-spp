from microquake.pipelines import automatic_pipeline
from microquake.clients.api_client import (get_events_catalog,
                                           put_data_from_objects)

from microquake.clients.ims import web_client
from microquake.core.settings import settings
from microquake.processors import (interloc, quick_magnitude, ray_tracer,
                                   event_classifier)
from obspy.core.event import ResourceIdentifier
from datetime import datetime
from loguru import logger
from microquake.core.event import Catalog, Event
import requests

api_base_url = settings.get('API_BASE_URL')

start_time = datetime(2019, 11, 1)
end_time = datetime(2019, 11, 16)

inventory = settings.inventory

res = get_events_catalog(api_base_url, start_time, end_time,
                         event_type='earthquake')

nb_event = len(res)
for i in range(len(res)):
    re = res[i]
    event_id = re.event_resource_id

    percent = (i + 1) / len(res)

    logger.info('{:0.2f}% - event {} of {}: {}'.format(percent, i+1,
                                                       nb_event, event_id))

    try:
        logger.info('downloading catalog')
        cat = re.get_event()
        cat.origins = []
        cat.magnitudes = []
        logger.info('downloading waveforms')
        st = re.get_waveforms()
        context = re.get_context_waveforms()
        vl = re.get_variable_length_waveforms()
    except KeyboardInterrupt:
        exit()
    except:
        continue

    # make sur the channel is upper case.
    for tr in st:
        tr.stats.channel = tr.stats.channel.upper()

    cat_auto = automatic_pipeline.automatic_pipeline(cat.copy(), st)
    logger.info('sending data to the api')

    response = put_data_from_objects(api_base_url, cat=cat_auto)
    print(response)
    input('bayaralala')
