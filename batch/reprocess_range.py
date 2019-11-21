from microquake.pipelines import automatic_pipeline
from microquake.clients.api_client import (get_events_catalog,
                                           put_data_from_objects)

# from microquake.clients.api_client import get_event_types
from microquake.clients.ims import web_client
from microquake.core.settings import settings
from microquake.processors import (interloc, quick_magnitude, ray_tracer,
                                   event_classifier)
from spp.data_connector.pre_processing import event_classification, get_event_types
from obspy.core.event import ResourceIdentifier
from datetime import datetime
from loguru import logger
from microquake.core.event import Catalog, Event
import requests

api_base_url = settings.get('API_BASE_URL')

start_time = datetime(2019, 3, 31)
end_time = datetime(2019, 11, 1)

inventory = settings.inventory

res = get_events_catalog(api_base_url, start_time, end_time,
                         event_type='earthquake')

event_types_lookup = get_event_types()
# event_types_lookup = get_event_types(api_base_url)

network = settings.network_code

nb_event = len(res)
for i in range(len(res)):
    re = res[i]
    event_id = re.event_resource_id
    logger.info(event_id)

    percent = (i + 1) / len(res) * 100

    logger.info('{:0.1f}% - event {} of {}: {}'.format(percent, i+1,
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
    
    interloc_processor = interloc.Processor()
    tmp = interloc_processor.process(stream=st)
    cat_interloc = interloc_processor.output_catalog(cat.copy())
    magnitude_processor = quick_magnitude.Processor()
    tmp = magnitude_processor.process(cat=cat_interloc.copy(),
                                      stream=st)
    cat_magnitude = magnitude_processor.output_catalog(cat_interloc.copy())

    cat_classified = event_classification(cat_magnitude.copy(), st, context, 
                                          event_types_lookup)[0]
   
    ray_tracer_processor = ray_tracer.Processor()
    tmp = ray_tracer_processor.process(cat=cat_classified.copy())
    cat_ray_trace = ray_tracer_processor.output_catalog(cat_classified.copy())
   
    cat_auto = automatic_pipeline.automatic_pipeline(cat_ray_trace.copy(), st)
    logger.info('sending data to the api')

    response = put_data_from_objects(api_base_url, network, cat=cat_auto)
    print(response)
