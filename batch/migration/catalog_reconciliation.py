from microquake.clients.ims import web_client
from microquake.clients import api_client
from microquake.core.settings import settings
from datetime import datetime, timedelta
from pytz import utc
from microquake.db.connectors import RedisQueue
from microquake.db.models.redis import set_event
from obspy.core.event import ResourceIdentifier
from loguru import logger
from obspy.core import UTCDateTime
import numpy as np
from microquake.processors import event_classifier, ray_tracer, nlloc
from microquake.core.event import Catalog
from obspy.core.event import ResourceIdentifier
from importlib import reload
import requests
from microquake.core.helpers.grid import get_grid_point
from microquake.core.helpers.time import get_time_zone

reload(web_client)


def prepare_context(event, st):

    distances = []
    stations = []

    for arr in event.preferred_origin().arrivals:
        distances.append(arr.distance)
        stations.append(arr.get_sta())

    i = np.argmin(distances)
    station = stations[i]

    trace = st.select(station=station).copy()

    # detrend and demean the trace
    trace = trace.detrend('linear').detrend('demean').taper(
        max_percentage=0.05, max_length=0.005)

    trace_start_time = trace[0].stats.starttime
    trace_end_time = trace[0].stats.endtime

    delta = (trace_end_time - trace_start_time) / 2

    trace_mid_time = trace_start_time + delta

    context = trace.trim(starttime=trace_mid_time-10,
                         endtime=trace_mid_time+10, pad=True, fill_value=0)

    return context


def prepare_stream(context, cat):
    pick_times = [arr.get_pick().time for arr in cat[
        0].preferred_origin().arrivals]
    np.min(pick_times)
    starttime = np.min(pick_times) - 0.5
    endtime = starttime + 2

    return context.copy().trim(starttime=starttime, endtime=endtime,
                               pad=True, fill_value=0)


def estimate_origin_time_and_time_residuals(cat):

    ev_loc = cat[0].preferred_origin().loc

    ots = []
    cat_out = cat.copy()
    for arr in cat[0].preferred_origin().arrivals:
        station = arr.get_pick().waveform_id.station_code
        phase = arr.phase
        tt = get_grid_point(station, phase, ev_loc)

        ots.append(arr.get_pick().time - tt)

    mean_diff = np.mean(np.diff(ots))

    ot = ots[0] + mean_diff
    cat_out[0].preferred_origin().time = ot

    for i, arr in enumerate(cat[0].preferred_origin().arrivals):
        station = arr.get_pick().waveform_id.station_code
        phase = arr.phase
        tt = get_grid_point(station, phase, ev_loc)
        tp = ot + get_grid_point(station, phase, ev_loc)
        residual = tp - arr.get_pick().time
        cat_out[0].preferred_origin().arrivals[i].time_residual = residual

    return cat_out.copy()


def process(event):
    if event.preferred_origin().evaluation_mode == 'automatic':
        logger.info('event automatically accepted... skipping!')
        return

    logger.info(f'getting picks for event: '
                f'{str(event.preferred_origin().time)}')
    event = web_client.get_picks_event(ims_base_url, event, inventory, '')

    if not event.preferred_origin().arrivals:
        logger.info('This event does not contains pick... skipping')
        return

    logger.info(f'getting waveforms')
    st = web_client.get_seismogram_event(ims_base_url, event, 'OT', '')
    context = prepare_context(event.copy(), st.copy())

    cat_tmp = Catalog(events=[event.copy()])

    logger.info('locating the event using nlloc')

    cat_tmp = nllp.process(cat=cat_tmp.copy())['cat']
    event_time_local = cat_tmp[0].preferred_origin().time.datetime.replace(
        tzinfo=utc).astimezone(get_time_zone())

    logger.info('categorizing event')
    # preparing the stream file
    # station = context[0].stats.station
    # cat_nlloc = cat_tmp.copy()
    # cat_tmp = cat_nlloc.copy()
    event_types = ecp.process(cat=cat_tmp.copy(), stream=st, context=context)

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

    cat_tmp[0].event_type = event_types_lookup[mq_event_type]

    event_type = event_types_lookup[mq_event_type]

    logger.info(f'the event was categorized as {mq_event_type}')

    # cat_tmp = estimate_origin_time_and_time_residuals(cat_tmp.copy())
    _ = rtp.process(cat=cat_tmp.copy())
    cat_tmp = rtp.output_catalog(cat_tmp.copy())
    origin_time = cat_tmp[0].preferred_origin().time

    cat_tmp[0].preferred_origin().time = origin_time

    cat_tmp[0].preferred_magnitude_id = cat[0].magnitudes[-1].resource_id
    cat_tmp[0].preferred_magnitude().origin_id = cat_tmp[
        0].preferred_origin().resource_id
    cat_tmp[0].preferred_origin().evaluation_mode = 'automatic'

    event_resource_id = cat_tmp[0].resource_id.id
    if api_client.get_event_by_id(api_base_url, event_resource_id):

        logger.info(f'an event with event id {event_resource_id} was found '
                    f'within the tolerance time.')

        cat_tmp[0].resource_id = ResourceIdentifier(event_resource_id)

        files = api_client.prepare_data(cat=cat_tmp.copy(),
                                        context=context.copy(),
                                        stream=st.copy(),
                                        variable_length=st.copy())

        patch_url = f'{api_base_url}events/{event_resource_id}'

        logger.info('patching the event!')
        response = requests.patch(patch_url, files=files.copy(),
                                  params={'send_to_bus': False},
                                  headers={'connection': 'close'})

    else:
        logger.info('no event was found within the tolerance time')
        logger.info('creating a new event')
        response = api_client.post_data_from_objects(api_base_url,
                                                     'OT',
                                                     event_id=None,
                                                     cat=cat_tmp.copy(),
                                                     stream=st.copy(),
                                                     context=context.copy(),
                                                     variable_length=st.copy(),
                                                     send_to_bus=False)

    logger.info(response)
    return response


api_base_url = settings.get('api_base_url')

# if api_base_url[-1] == '/':
#     api_base_url = api_base_url[:-1]


ims_base_url = settings.get('ims_base_url')

event_types_lookup = api_client.get_event_types(api_base_url)

# will look at the last day of data
end_time = datetime.utcnow() - timedelta(hours=1)

# looking at the events for the last month

start_time = UTCDateTime(2019, 4, 30)
end_time = UTCDateTime(2019, 5, 2)

inventory = settings.inventory

cat = web_client.get_catalogue(ims_base_url, start_time, end_time, inventory,
                               utc, blast=True, event=True, accepted=True,
                               manual=True, get_arrivals=False)
ct = 0

sorted_cat = sorted(cat, reverse=True,
                    key=lambda x: x.preferred_origin().time)

ecp = event_classifier.Processor()
nllp = nlloc.Processor()
rtp = ray_tracer.Processor()

for i, event in enumerate(sorted_cat):

    logger.info(f'processing event {i + 1} of {len(cat)} -- '
                f'{(i + 1)/len(cat) * 100}%)')

    response = process(event)


    # rastapopoulos


