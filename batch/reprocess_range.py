from microquake.pipelines import automatic_pipeline
from microquake.clients.api_client import get_events_catalog
from microquake.clients.ims import web_client
from microquake.core.settings import settings
from microquake.processors import interloc, quick_magnitude, ray_tracer
from datetime import datetime
from loguru import logger

api_base_url = settings.get('API_BASE_URL')

start_time = datetime(2019, 1, 1)
end_time = datetime(2019, 8, 1)

ims_base_url = settings.get('ims_base_url')
inventory = settings.inventory

# ims_cat = web_client.get_catalogue(ims_base_url, start_time, end_time,
#                                    inventory, '')

# ims_event = web_client.get_catalogue(start_time, end_time, )

res = get_events_catalog(api_base_url, start_time, end_time)
res_rejected = get_events_catalog(api_base_url, start_time, end_time,
                                  status='rejected')

ilp = interloc.Processor()
qmp = quick_magnitude.Processor()
rtp = ray_tracer.Processor()

nb_event = len(res)
for i in range(len(res)):
    re = res[i]
    event_id = re.event_resource_id

    percent = i / len(res)

    logger.info('{:0.2f}% - event {} of {}: {}'.format(percent, i,
                                                       nb_event, event_id))
    logger.info('downloading catalog')
    cat = re.get_event()
    logger.info('downloading waveforms')
    st = re.get_waveforms()

    cat_interloc = ilp.process(stream=st)
    cat_qm = qmp.process(cat=cat_interloc, stream=st)
    cat_rt = rtp.process(cat=cat_qm)

    logger.info('automatic processing')
    cat_out = automatic_pipeline.automatic_pipeline_processor(cat_rt, st)

    logger.info('putting data to the api')
    automatic_pipeline.put_data_processor(cat_out)

