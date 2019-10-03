from microquake.pipelines import automatic_pipeline
from microquake.clients.api_client import (get_events_catalog,
                                           post_data_from_objects,
                                           put_data_from_objects)
from microquake.clients.ims import web_client
from microquake.core.settings import settings
from microquake.processors import interloc, quick_magnitude, ray_tracer
from obspy.core.event import ResourceIdentifier
from datetime import datetime
from loguru import logger
from microquake.core.event import Catalog, Event

api_base_url = settings.get('API_BASE_URL')

start_time = datetime(2019, 6, 1)
end_time = datetime(2019, 7, 10)

ims_base_url = settings.get('ims_base_url')
inventory = settings.inventory

# ims_cat = web_client.get_catalogue(ims_base_url, start_time, end_time,
#                                    inventory, '')

# ims_event = web_client.get_catalogue(start_time, end_time, )

res = get_events_catalog(api_base_url, start_time, end_time,
                         event_type='earthquake')
res_rejected = get_events_catalog(api_base_url, start_time, end_time,
                                  status='rejected')

ilp = interloc.Processor()
qmp = quick_magnitude.Processor()
rtp = ray_tracer.Processor()

nb_event = len(res)
for i in range(len(res)):
    re = res[i]
    event_id = re.event_resource_id

    percent = (i + 1) / len(res)

    logger.info('{:0.2f}% - event {} of {}: {}'.format(percent, i+1,
                                                       nb_event, event_id))
    logger.info('downloading catalog')
    cat = re.get_event()
    cat.origins = []
    cat.magnitudes = []
    logger.info('downloading waveforms')
    st = re.get_waveforms()
    context = re.get_context_waveforms()
    vl = re.get_variable_length_waveforms()

    tmp = ilp.process(stream=st)
    cat_interloc = ilp.output_catalog(Catalog(events=[Event()]))
    tmp = qmp.process(cat=cat_interloc.copy(), stream=st)
    cat_qm = qmp.output_catalog(cat_interloc.copy())
    tmp = rtp.process(cat=cat_qm.copy())
    cat_rt = rtp.output_catalog(cat_qm.copy())

    logger.info('automatic processing')
    cat_out = automatic_pipeline.automatic_pipeline_processor(cat_rt.copy(),
                                                              st)

    # from ipdb import set_trace; set_trace()

    logger.info('putting data to the api')

    # post_data_from_objects(api_base_url, event_id=event_id, event=cat_out,
    #                        stream=st, context_stream=context,
    #                        variable_length_stream=vl, tolerance=0.5,
    #                        send_to_bus=False)

    # automatic_pipeline.put_data_processor(cat_out)

    api_base_url = settings.get('API_BASE_URL')

    event_time = cat_out[0].preferred_origin().time
    context_start_time = context[0].stats.starttime
    context_end_time = context[0].stats.endtime
    if context_end_time - context_start_time < 10:
        min_dist = 1e10
        sensor = ''
        for ray in cat_out[0].preferred_origin().rays:

            if ray.station_code in settings.get('sensors').black_list:
                continue
            if ray.length > min_dist:
                continue
            if len(st.select(station=ray.station_code)) == 0:
                continue
            min_dist = ray.length
            sensor = ray.station_code

        context = st.select(station=sensor).copy()

    context = context.taper(max_percentage=0.01)
    context = context.trim(starttime=event_time-10, endtime=event_time+10,
                           pad=True, fill_value=0)

    logger.info(f'event time {cat_out[0].preferred_origin().time}')
    post_data_from_objects(api_base_url, cat=cat_out, stream=st,
                           context=context, variable_length=vl, tolerance=None)
    # from ipdb import set_trace; set_trace()

