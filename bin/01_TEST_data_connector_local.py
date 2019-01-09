from microquake.IMS import web_client
from microquake.core import UTCDateTime, read_events, read
from microquake.core.event import Catalog
from spp.utils.application import Application
import pytz
import os
from io import BytesIO
from spp.utils import seismic_client
import numpy as np
from time import time
from IPython.core.debugger import Tracer
from importlib import reload
from spp.utils import seismic_client
reload(web_client)
# import time
from glob import glob

# colorama.init()

__module_name__ = 'initializer'

app = Application(module_name=__module_name__, processing_flow='automatic',
                  init_processing_flow=True)
app.init_module()

logger = app.get_logger('data_connector', 'data_connector.log')

site = app.get_stations()
ims_base_url = app.settings.data_connector.path
end_time = UTCDateTime.now() - 3600
start_time = end_time - 5 * 24 * 3600  # 4 days
tz = app.get_time_zone()

end_time = end_time.datetime.replace(tzinfo=tz)
start_time = start_time.datetime.replace(tzinfo=tz)

logger.info('Retrieving event the seismic API')
t0 = time()
base_url = app.settings.seismic_api.base_url
catalog = seismic_client.get_events_catalog(base_url, start_time, end_time)
t1 = time()
logger.info('Done retrieving event from the web api server. \n Retrived %d '
            'event, in %0.3f seconds' % (len(catalog), t1 - t0))

spp_home = os.environ['SPP_HOME']

for request_event in catalog:
    if request_event.z > 500:
        continue
    time_utc = request_event.time_utc
    logger.info('Retrieving catalog information for %s from seismic API' %
                time_utc)
    t0 = time()
    cat = request_event.get_event()
    t1 = time()
    logger.info('Done retrieving event in %0.3f seconds' % (t1 - t0))

    logger.info('Retrieving waveforms from seismic API')
    t0 = time()
    wf = request_event.get_waveforms()
    t1 = time()
    logger.info('Done retrieving waveform from seismic API in %0.3f' % (t1 -
                                                                        t0))

    app.send_message(cat, wf)


# for event in cat_ims:
#     if event.preferred_origin().z > 500:
#         continue
#     fname = str(event.origins[0].time) + '.xml'
#     fpath = os.path.join(spp_home, 'results', fname)
#
#     if glob(fpath):
#         # event = read_events(fpath)[0]
#         event = web_client.get_picks_event(ims_base_url, event, site, tz)
#         wf = read(fpath.replace('.xml', '.mseed'))
#
#     else:
#
#         logger.info('extracting data for event %s' % str(event))
#         event_time = event.preferred_origin().time
#         st = event_time - 1
#         et = event_time + 1
#         event = web_client.get_picks_event(ims_base_url, event, site, tz)
#         c_wf = web_client.get_continuous(ims_base_url, st, et, site_ids, tz)
#         arrival_times = [arrival.get_pick().time for arrival in
#                          event.preferred_origin().arrivals]
#
#
#         if not c_wf:
#             # wf = web_client.get_seismogram_event(ims_base_url, event, 'OT', tz)
#             # wf = wf.write(fpath.replace('xml', 'mseed'), format='MSEED')
#             # context = None
#             continue
#         else:
#             wf = c_wf.copy().trim(starttime=event_time-0.2, endtime=event_time+1.)
#             wf.write(fpath.replace('xml', 'mseed'))
#             index = np.argmin([arrival.distance for arrival in
#                                event.preferred_origin().arrivals])
#             station_code = event.preferred_origin().arrivals[index
#                            ].get_pick().waveform_id.station_code
#
#             context = c_wf.select(station=station_code).composite()
#             context.write(fpath.replace('.xml', '_context.mseed'), format='MSEED')
#
#         event.write(fpath, format='QUAKEML')
#
#     logger.info('extracting data for event %s' % str(event))
#     arrival_times = [arrival.get_pick().time for arrival in
#                      event.preferred_origin().arrivals]
#     # event_time = np.min(arrival_times)
#     # st = event_time - 1
#     # et = event_time + 1
#
#
#
#     # origin[0] is used here to ensure that the filename is consistent
#     # throughout.
#
#     # fname = str(event.origins[0].time) + '.xml'
#     #
#     # event.write(fpath, format='QUAKEML')
#     #
#     # if not c_wf:
#     #     wf = web_client.get_seismogram_event(ims_base_url, event, 'OT', tz)
#     #     wf = wf.write(fpath.replace('xml', 'mseed'), format='MSEED')
#     #     context = None
#     # else:
#     #     wf = c_wf.copy().trim(starttime=event_time-0.2, endtime=event_time+1.)
#     #     wf.write(fpath.replace('xml', 'mseed'))
#     #     index = np.argmin([arrival.distance for arrival in
#     #                        event.preferred_origin().arrivals])
#     #
#     #     station_code = event.preferred_origin().arrivals[index
#     #                    ].get_pick().waveform_id.station_code
#     #
#     #     context = c_wf.select(station=station_code).composite()
#     #     context.write(fpath.replace('.xml', '_context.mseed'), format='MSEED')
#
#
#     logger.info('sending message')
#     app.send_message(Catalog(events=[event]), wf)
#     logger.info('done sending message')
#     # input('press a key to continue')
#
#     # logger.info('uploading the data to the server (url:%s)' % api_base_url)
#     # t0 = time()
#     # seismic_client.post_data_from_objects(api_base_url, event_id=None,
#     #                                       event=event, stream=wf,
#     #                                       context_stream=context)
#     # t1 = time()
#     # logger.info('done uploading the data to the server in %0.3f seconds'
#     #             % (t1 - t0))
#     #
#     # logger.info('uploading continuous data to the server (url:%s'
#     #             % api_base_url)
#     # t2 = time()
#     # seismic_client.post_continuous_stream(api_base_url, c_wf,
#     #                                       post_to_kafka=True,
#     #                                       stream_id=event.resource_id)
#     # t3 = time()
#     # logger.info('done uploading continuous data in %0.3f seconds' % (t3 - t2))
#
#
#















