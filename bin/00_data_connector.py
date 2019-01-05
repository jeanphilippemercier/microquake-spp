from microquake.IMS import web_client
from microquake.core import UTCDateTime
from spp.utils.application import Application
import pytz
import os
import colorama
from io import BytesIO
from spp.utils import seismic_client
import numpy as np
from time import time
from IPython.core.debugger import Tracer

colorama.init()

__module_name__ = 'data_connector'

app = Application(module_name=__module_name__)
# app.init_module()

logger = app.get_logger('data_connector', 'data_connector.log')

site = app.get_stations()
ims_base_url = app.settings.data_connector.path
end_time = UTCDateTime.now() - 3600
start_time = end_time - 4 * 24 * 3600  # 4 days
tz = app.get_time_zone()

end_time = end_time.datetime.replace(tzinfo=tz)
start_time = start_time.datetime.replace(tzinfo=tz)

cat_ims = web_client.get_catalogue(ims_base_url, start_time, end_time,
                                   site, tz, blast=False)

api_base_url = app.settings.seismic_api.base_url
cat_mq = seismic_client.get_events_catalog(api_base_url, start_time, end_time)

# check if event is already in the database

events_mq = [request_event.get_event() for request_event in cat_mq]

event_to_upload = [evt_ims for evt_ims in cat_ims]

for event_ims in cat_ims:
    event_time = event_ims.preferred_origin().time
    to_db = True
    for cat_mq in events_mq:
        event_mq = cat_mq[0]
        if np.abs(event_mq.preferred_origin().time - event_time) < 0.5:
            to_db = False

    if to_db:
        event_to_upload.append(event_ims)

site_ids = [int(station.code) for station in site.stations()]

for event in event_to_upload:
    event = web_client.get_picks_event(ims_base_url, event, site, tz)

    logger.info('extracting data for event %s' % str(event))
    event_time = event.preferred_origin().time - 10
    st = event_time - 1
    et = event_time + 1
    c_wf = web_client.get_continuous(ims_base_url, st, et, site_ids, tz)

    if not c_wf:
        wf = web_client.get_seismogram_event(ims_base_url, event, 'OT', tz)
        context = None
    else:
        wf = c_wf.copy().trim(starttime=event_time-0.2, endtime=event_time+1.)
        index = np.argmin([arrival.distance for arrival in
                           event.preferred_origin().arrivals])

        station_code = event.preferred_origin().arrivals[index
                       ].get_pick().waveform_id.station_code

        context = c_wf.select(station=station_code).composite()

    logger.info('uploading the data to the server (url:%s)' % api_base_url)
    t0 = time()
    seismic_client.post_data_from_objects(api_base_url, event_id=None,
                                          event=event, stream=wf,
                                          context_stream=context)
    t1 = time()
    logger.info('done uploading the data to the server in %0.3f seconds'
                % (t1 - t0))

    logger.info('uploading continuous data to the server (url:%s'
                % api_base_url)
    t2 = time()
    seismic_client.post_continuous_stream(api_base_url, c_wf,
                                          post_to_kafka=True,
                                          stream_id=event.resource_id)
    t3 = time()
    logger.info('done uploading continuous data in %0.3f seconds' % (t3 - t2))


















