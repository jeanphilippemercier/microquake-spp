from microquake.clients.ims import web_client
from microquake.clients import api_client
from microquake.core.settings import settings
from datetime import datetime, timedelta
from pytz import utc
from microquake.db.connectors import RedisQueue
from microquake.db.models.redis import set_event
from obspy.core.event import ResourceIdentifier
from microquake.helpers.logging import logger
from obspy.core import UTCDateTime
import numpy as np
from microquake.processors import event_classifier, ray_tracer, nlloc
from microquake.core.event import Catalog
from obspy.core.event import ResourceIdentifier
from importlib import reload
import requests
from microquake.helpers.grid import get_grid_point
from microquake.helpers.time import get_time_zone

reload(web_client)


def prepare_context(evt, st):

    distances = []
    stations = []

    for arr in evt.preferred_origin().arrivals:
        distances.append(arr.distance)
        stations.append(arr.get_sta())
    #
    i = np.argmin(distances)
    indices = np.argsort(distances)
    station = stations[i]

    for i in indices:
        station = stations[i]
        trace = st.select(station=station).copy()

        # detrend and demean the trace
        trace = trace.detrend('linear').detrend('demean').taper(
            max_percentage=0.05, max_length=0.005)

        ot = evt.preferred_origin().time

        trace_start_time = ot - 10
        trace_end_time = ot + 10

        # delta = (trace_end_time - trace_start_time) / 2

        # trace_mid_time = trace_start_time + delta

        context = trace.trim(starttime=trace_start_time,
                             endtime=trace_end_time, pad=True, fill_value=0)

        if context is not None:
            break

    return context


def prepare_stream(context, cat):
    pick_times = [arr.get_pick().time for arr in cat[
        0].preferred_origin().arrivals]
    np.min(pick_times)
    starttime = np.min(pick_times) - 0.5
    endtime = starttime + 2

    return context.copy().trim(starttime=starttime, endtime=endtime,
                               pad=True, fill_value=0)


def estimate_origin_time_and_time_residuals(cata):

    ev_loc = cata[0].preferred_origin().loc

    ots = []
    cat_out = cata.copy()
    for arr in cata[0].preferred_origin().arrivals:
        station = arr.get_pick().waveform_id.station_code
        phase = arr.phase
        tt = get_grid_point(station, phase, ev_loc)

        ots.append(arr.get_pick().time - tt)

    if len(ots) == 0:
        mean_diff = 0
    else:
        mean_diff = np.mean(np.diff(ots))

    return ots[0] + mean_diff
    # cat_out[0].preferred_origin().time = ot
    #
    # for i, arr in enumerate(cata[0].preferred_origin().arrivals):
    #     station = arr.get_pick().waveform_id.station_code
    #     phase = arr.phase
    #     tt = get_grid_point(station, phase, ev_loc)
    #     tp = ot + get_grid_point(station, phase, ev_loc)
    #     residual = tp - arr.get_pick().time
    #     cat_out[0].preferred_origin().arrivals[i].time_residual = residual


def process(evt):
    if evt.preferred_origin().evaluation_mode == 'automatic':
        logger.info('event automatically accepted... skipping!')
        return

    logger.info(f'getting picks for event: '
                f'{str(evt.preferred_origin().time)}')
    evt = web_client.get_picks_event(ims_base_url, evt, inventory, '')

    if not evt.preferred_origin().arrivals:
        logger.info('This event does not contains pick... skipping')
        return

    logger.info(f'getting waveforms')
    st = web_client.get_seismogram_event(ims_base_url, evt, 'OT', '')

    cat_tmp = Catalog(events=[evt.copy()])

    try:
        event_time = estimate_origin_time_and_time_residuals(cat_tmp.copy())
    except:
        return

    tolerance = 1

    start_time = event_time - tolerance
    end_time = event_time + tolerance

    re_list = api_client.get_events_catalog(api_base_url, start_time,
                                            end_time, status='accepted,'
                                                             'rejected')

    if re_list:
        logger.info('event already exists... but continuing')
        # return

    logger.info('locating the event using nlloc')

    cat_nlloc = nllp.process(cat=cat_tmp.copy())['cat']
    event_time_local = cat_nlloc[0].preferred_origin().time.datetime.replace(
        tzinfo=utc).astimezone(get_time_zone())
    context = prepare_context(cat_nlloc[0].copy(), st.copy())

    if context is None:
        return

    logger.info('categorizing event')
    # preparing the stream file
    # station = context[0].stats.station
    # cat_nlloc = cat_tmp.copy()
    # cat_tmp = cat_nlloc.copy()
    try:
        event_types = ecp.process(cat=cat_nlloc.copy(), stream=st,
                                  context=context)
    except:
        return

    sorted_event_types = sorted(event_types.items(), reverse=True,
                                key=lambda x: x[1])

    blast_window_starts = settings.get('event_classifier').blast_window_starts
    blast_window_ends = settings.get('event_classifier').blast_window_ends

    hour = event_time_local.hour

    mq_event_type = sorted_event_types[0][0]

    ug_blast = False
    if mq_event_type == 'blast':
        for bws, bwe in zip(blast_window_starts, blast_window_ends):
            if bws <= hour < bwe:
                ug_blast = True

        if ug_blast:
            mq_event_type = 'underground blast'
        else:
            mq_event_type = 'other blast'

    cat_nlloc[0].event_type = event_types_lookup[mq_event_type]

    # event_type = event_types_lookup[mq_event_type]

    logger.info(f'the event was categorized as {mq_event_type}')

    # cat_tmp = estimate_origin_time_and_time_residuals(cat_tmp.copy())
    _ = rtp.process(cat=cat_nlloc.copy())
    cat_ray = rtp.output_catalog(cat_nlloc.copy())
    origin_time = cat_ray[0].preferred_origin().time

    cat_ray[0].preferred_origin().time = origin_time

    cat_ray[0].preferred_magnitude_id = cat_ray[0].magnitudes[-1].resource_id
    cat_ray[0].preferred_magnitude().origin_id = cat_ray[
        0].origins[-1].resource_id
    cat_ray[0].preferred_origin().evaluation_mode = 'automatic'

    cat_ray[0].resource_id = ResourceIdentifier()

    event_resource_id = cat_ray[0].resource_id.id
    if api_client.get_event_by_id(api_base_url, event_resource_id):

        logger.info(f'an event with event id {event_resource_id} was found '
                    f'within the tolerance time.')

        cat_ray[0].resource_id = ResourceIdentifier(event_resource_id)

        files = api_client.prepare_data(cat=cat_ray.copy(),
                                        context=context.copy(),
                                        stream=st.copy(),
                                        variable_length=st.copy())

        patch_url = f'{api_base_url}events/{event_resource_id}'

        logger.info('patching the event!')
        response = requests.patch(patch_url, files=files.copy(),
                                  params={'send_to_bus': False},
                                  headers={'connection': 'close'},
                                  timeout=20)

    else:
        logger.info('no event was found within the tolerance time')
        logger.info('creating a new event')
        post_url = f'{api_base_url}events'
        files = api_client.prepare_data(cat=cat_ray.copy(),
                                        context=context.copy(),
                                        stream=st.copy(),
                                        variable_length=st.copy())
        response = requests.post(post_url, files=files.copy(),
                                 params={'send_to_bus': False})
        # response = api_client.post_data_from_objects(api_base_url,
        #                                              'OT',
        #                                              event_id=None,
        #                                              cat=cat_ray.copy(),
        #                                              stream=st.copy(),
        #                                              context=context.copy(),
        #                                              variable_length=st.copy(),
        #                                              send_to_bus=False)

    logger.info(response)
    del cat_tmp
    del cat_nlloc
    del cat_ray
    del st
    del context
    return response


api_base_url = settings.get('api_base_url')

# if api_base_url[-1] == '/':
#     api_base_url = api_base_url[:-1]


ims_base_url = settings.get('ims_base_url')

event_types_lookup = api_client.get_event_types(api_base_url)

# will look at the last day of data
end_time = datetime.utcnow() - timedelta(hours=1)

# looking at the events for the last month

start_time = UTCDateTime(2017, 1, 1, 0, 0, 0)
# 2019-02-24T05:19:31.704817Z
# 2018-11-13T23:07:56.893230Z
# 2018-10-13T11:15:59.593463Z
# 2018-08-27T23:29:41.520066Z
# 2018-06-27T18:29:20.708693Z
# 2018-04-07T23:15:03.545375Z
# 2018-03-23T22:57:33.094229Z
# 2018-03-01T21:20:18.321224
# 2018-02-21T15:24:48.090395Z
# 2018-02-15T22:35:05.260772Z
# 2018-02-11T02:10:11.723759Z
# 2018-01-25T23:15:18.531314Z
# 2018-01-06T19:59:49.797682Z
# 2018-01-01T10:42:46.530023Z
# 2017-12-22T20:59:09.267085Z
# 2017-12-17T11:03:46.931079Z
# 2017-12-03T19:58:53.303239Z
# 2017-11-25T02:19:22.648916Z
# 2017-11-02T10:12:15.188809Z
# 2017-10-25T02:10:25.877681Z
#2017-10-24T22:43:14.353598Z
end_time = UTCDateTime(2017, 10, 24, 22, 43, 0)

inventory = settings.inventory

cat = web_client.get_catalogue(ims_base_url, start_time, end_time, inventory,
                               utc, blast=True, event=True, accepted=True,
                               manual=True, get_arrivals=False)

res = api_client.get_events_catalog(api_base_url, start_time, end_time,
                                    status='accepted, rejected')

ct = 0

sorted_cat = sorted(cat, reverse=True,
                    key=lambda x: x.preferred_origin().time)

ecp = event_classifier.Processor()
nllp = nlloc.Processor()
rtp = ray_tracer.Processor()

for i, event in enumerate(sorted_cat):

    logger.info(f'processing event {i + 1} of {len(cat)} -- '
                f'{(i + 1)/len(cat) * 100}%)')

    response = process(event.copy())


    # rastapopoulos


