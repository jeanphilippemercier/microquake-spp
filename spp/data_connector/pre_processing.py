"""
if the waveform extraction fails, resend to the queue!
"""

from datetime import datetime, timedelta
from time import time

import numpy as np
import requests
from pytz import utc

from loguru import logger
from microquake.clients.ims import web_client
from microquake.clients.api_client import post_data_from_objects
from obspy import UTCDateTime
from microquake.core.settings import settings
from microquake.db.connectors import RedisQueue, record_processing_logs_pg
from microquake.db.models.redis import set_event, get_event
from microquake.processors import (clean_data, event_classifier, interloc,
                                   quick_magnitude, ray_tracer)
from microquake.core.helpers.timescale_db import (get_continuous_data,
                                                  get_db_lag)
from microquake.core.helpers.time import get_time_zone
import json

automatic_message_queue = settings.AUTOMATIC_PIPELINE_MESSAGE_QUEUE
try:
    automatic_job_queue = RedisQueue(automatic_message_queue)
except ValueError as e:
    logger.error(e)

api_message_queue = settings.API_MESSAGE_QUEUE

try:
    api_job_queue = RedisQueue(api_message_queue)
except ValueError as e:
    logger.error(e)

we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE

try:
    we_job_queue = RedisQueue(we_message_queue)
except ValueError as e:
    logger.error(e)

__processing_step__ = 'pre_processing'
__processing_step_id__ = 2

sites = [int(station.code) for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')
api_base_url = settings.get('api_base_url')
inventory = settings.inventory
network_code = settings.NETWORK_CODE

# tolerance for how many trace are not recovered
minimum_recovery_fraction = settings.get(
    'data_connector').minimum_recovery_fraction


def get_event_types():

    url = api_base_url + 'inventory/microquake_event_types'
    response = requests.get(url)

    if not response:
        raise ConnectionError('API Connection Error')

    data = json.loads(response.content)
    dict_out = {}
    for d in data:
        dict_out[d['microquake_type']] = d['quakeml_type']

    return dict_out


def extract_continuous(starttime, endtime, sensor_id=None):
    st = get_continuous_data(starttime, endtime, sensor_id)
    # st = None

    if sensor_id is not None:
        sensors = [sensor_id]
    else:
        sensors = sites

    if st is None:
        logger.warning('request of the continuous data from the '
                       'TimescaleDB returned None... requesting data from '
                       'the IMS system through the web API instead!')

        logger.warning(f'the database lag is {get_db_lag()} seconds')

        st = web_client.get_continuous(base_url, starttime, endtime,
                                       sensors, utc, network=network_code)

    recovery_ratio = len(inventory.stations()) / len(st.unique_stations())
    if recovery_ratio < 0.5:
        logger.warning('request of the continuous data from the '
                       'TimescaleDB returned an insufficient number '
                       'of traces (less than 50%)... requesting the data from '
                       'the IMS system through the web API instead!')

        st = web_client.get_continuous(base_url, starttime, endtime,
                                       sensors, utc, network=network_code)

    return st


def interloc_election(cat):

    event = cat[0]

    event_time = event.preferred_origin().time.datetime.replace(
        tzinfo=utc)

    interloc_results = []
    thresholds = []
    wfs = []

    starttime = event_time - timedelta(seconds=1.5)
    endtime = event_time + timedelta(seconds=1.5)

    complete_wf = extract_continuous(starttime, endtime)

    complete_wf.detrend('demean').taper(max_percentage=0.001,
                                        max_length=0.01).filter('bandpass',
                                                                freqmin=60,
                                                                freqmax=500)

    for offset in [-1.5, -1, -0.5]:
        starttime = event_time + timedelta(seconds=offset)
        endtime = starttime + timedelta(seconds=2)

        wf = complete_wf.copy()
        wf.trim(starttime=UTCDateTime(starttime),
                endtime=UTCDateTime(endtime))
        wf = wf.detrend('demean').taper(max_length=0.01, max_percentage=0.01)

        clean_data_processor = clean_data.Processor()
        clean_wf = clean_data_processor.process(waveform=wf)

        max_len = np.max([len(tr) for tr in clean_wf])
        trs = [tr for tr in clean_wf if len(tr) == max_len]
        clean_wf.traces = trs

        interloc_processor = interloc.Processor()
        results = interloc_processor.process(stream=clean_wf)
        new_cat = interloc_processor.output_catalog(cat)
        results['catalog'] = new_cat
        interloc_results.append(results)
        wfs.append(wf)
        thresholds.append(results['normed_vmax'])

    index = np.argmax(thresholds)

    logger.info('Event location: {}'.format(new_cat[0].preferred_origin().loc))
    logger.info('Event time: {}'.format(new_cat[0].preferred_origin().time))

    return interloc_results[index]


def get_waveforms(interloc_dict, event):

    utc_time = datetime.fromtimestamp(interloc_dict['event_time'])
    local_time = utc_time.replace(tzinfo=utc)
    starttime = local_time - timedelta(seconds=0.5)
    endtime = local_time + timedelta(seconds=1.5)

    fixed_length_wf = extract_continuous(starttime, endtime)
    clean_data_processor = clean_data.Processor()
    fixed_length_wf = clean_data_processor.process(waveform=fixed_length_wf)

    starttime = local_time - timedelta(seconds=10)
    endtime = local_time + timedelta(seconds=10)
    dists = []
    stations = []
    ev_loc = np.array([interloc_dict['x'], interloc_dict['y'],
                       interloc_dict['z']])

    for station in inventory.stations():
        if len(fixed_length_wf.select(station=station.code)) == 0:
            continue
        station_data = fixed_length_wf.select(station=station.code)[0].data
        sampling_rate = fixed_length_wf.select(station=station.code)[
            0].stats.sampling_rate
        expected_number_sample = (endtime - starttime).total_seconds() * \
            sampling_rate

        if len(station_data) < expected_number_sample * 0.95:
            if np.isnan(station_data).any():
                continue
        dists.append(np.linalg.norm(ev_loc - station.loc))
        stations.append(station.code)

    indices = np.argsort(dists)

    context_trace_filter = settings.get('data_connector').context_trace.filter

    for i in indices:
        if stations[i] in settings.get('sensors').black_list:
            continue
        if not inventory.select(stations[i]):
            continue

        logger.info(f'getting context trace for station {stations[i]} located '
                    f'at {dists[i]} m from the event hypocenter')

        context = web_client.get_continuous(base_url, starttime, endtime,
                                            [stations[i]], utc,
                                            network=network_code)

        context.filter('bandpass', **context_trace_filter)

        if context:
            break
        else:
            logger.warning('context trace malformed continuing')

    waveforms = {'fixed_length': fixed_length_wf,
                 'context': context}

    return waveforms


def send_to_api(event_id, **kwargs):

    processing_step = 'post_event_api'
    processing_step_id = 4
    start_processing_time = time()
    send_to_bus = kwargs.get('send_to_bus', True)

    api_base_url = settings.get('api_base_url')
    event = get_event(event_id)

    if event is None:
        logger.error(f'The event {event_id} is not available anymore, exiting')
        return

    if event['attempt_number'] > 5:
        logger.warning('maximum number of attempt (5) reached. The event '
                       'will not be posted to the API')
        return

    if 'attempt_number' in event.keys():
        event['attempt_number'] += 1

    else:
        event['attempt_number'] = 1

    try:
        response = post_data_from_objects(api_base_url, event_id=None,
                                          cat=event['catalogue'],
                                          stream=event['fixed_length'],
                                          context=event['context'],
                                          variable_length=event['variable_length'],
                                          tolerance=None,
                                          send_to_bus=send_to_bus)
    except requests.exceptions.ConnectionError as e:
        logger.error(e)
        # logger.info('request failed, resending to queue')

        # set_event(event_id, **event)

        # result = api_job_queue.submit_task(send_to_api, event_id=event_id)

    if not response:
        logger.info('request failed')
        # logger.info('request failed, resending to the queue')
        # set_event(event_id, **event)
        # result = api_job_queue.submit_task(send_to_api, event_id=event_id)

    logger.info('request successful')
    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time

    evt = event['catalogue'][0]

    record_processing_logs_pg(evt, 'success', processing_step,
                              processing_step_id, processing_time)

    return response


def event_classification(cat, fixed_length, context, event_types_lookup):

    category = event_classifier.Processor().process(stream=fixed_length,
                                                    context=context,
                                                    cat=cat)

    sorted_list = sorted(category.items(), reverse=True,
                         key=lambda x: x[1])

    elevation = cat[0].preferred_origin().z
    maximum_event_elevation = settings.get(
        'data_connector').maximum_event_elevation

    logger.info('{}'.format(sorted_list))

    event_type = sorted_list[0][0]
    likelihood = sorted_list[0][1]

    likelihood_threshold = settings.get(
        'event_classifier').likelihood_threshold
    accepted_event_types = settings.get(
        'event_classifier').valid_event_types.to_list()
    uploaded_event_types = settings.get(
        'event_classifier').uploaded_event_types.to_list()
    blast_window_starts = settings.get('event_classifier').blast_window_starts
    blast_window_ends = settings.get('event_classifier').blast_window_ends

    automatic_processing = False
    save_event = False

    event_time_local = cat[0].preferred_origin().time.datetime.replace(
        tzinfo=utc).astimezone(get_time_zone())
    hour = event_time_local.hour

    ug_blast = False
    if event_type in ['blast']:
        for bws, bwe in zip(blast_window_starts, blast_window_ends):
            if bws <= hour < bwe:
                ug_blast = True

        if ug_blast:
            event_type = 'underground blast'
        else:
            event_type = 'other blast'

    # from ipdb import set_trace; set_trace()

    if event_type in accepted_event_types:

        cat[0].event_type = event_types_lookup[event_type]
        if likelihood >= likelihood_threshold:
            cat[0].preferred_origin().evaluation_status = 'preliminary'
            logger.info(f'event categorized as {event_type} will be further '
                        f'processed')
            automatic_processing = True
            save_event = True
        else:
            cat[0].preferred_origin().evaluation_status = 'rejected'
            logger.info(f'event categorized as {event_type} but with a '
                        f'likelihood of {likelihood} which is lower than '
                        f'the threshold {likelihood_threshold}. The event '
                        f'will be uploaded to the API and will be '
                        f'further processed ')
            automatic_processing = True
            save_event = True

    elif (sorted_list[1][0] in accepted_event_types) and \
            (sorted_list[1][1] > likelihood_threshold / 2):
        cat[0].event_type = event_types_lookup[event_type]
        cat[0].preferred_origin().evaluation_status = 'rejected'

        logger.info(f'event categorized as {event_type} but the event could '
                    f'also be {sorted_list[1][0]} with a likelihood of '
                    f'{sorted_list[1][1]}. The event will be marked '
                    f'as rejected but uploaded to the API. The event will '
                    f'be further processed.')
        automatic_processing = True
        save_event = True

    elif event_type in uploaded_event_types:
        if likelihood >= likelihood_threshold:
            cat[0].event_type = event_types_lookup[event_type]
            cat[0].preferred_origin().evaluation_status = 'rejected'
            logger.info(f'event categorized as {event_type}. The event will be'
                        f' marked as rejected but will not be further '
                        f'processed')
            save_event = True

    else:
        logger.info(f'event categorized as {event_type}. The event will not '
                    f'be send to the API nor be further processed.')
        cat[0].event_type = event_types_lookup[event_type]
        cat[0].preferred_origin().evaluation_status = 'rejected'

    return cat, automatic_processing, save_event


def pre_process(event_id, force_send_to_api=False,
                force_send_to_automatic=False, **kwargs):

    start_processing_time = time()

    logger.info('message received')

    # tmp = deserialize(message)
    start_processing_time = time()
    event = get_event(event_id)
    cat = event['catalogue']

    try:
        event_types_lookup = get_event_types()
    except ConnectionError:
        logger.error('api connection error, resending to the queue')
        set_event(event_id, **event)
        we_job_queue.submit_task(pre_process, event_id=event_id)

    try:
        variable_length_wf = web_client.get_seismogram_event(base_url, cat[0],
                                                             network_code, utc)
    except AttributeError:
        logger.warning('could not retrieve the variable length waveforms')
        variable_length_wf = None

    interloc_results = interloc_election(cat)

    if not interloc_results:
        logger.error('interloc failed')

        return

    new_cat = interloc_results['catalog']
    new_cat[0].preferred_origin().evaluation_status = 'preliminary'
    waveforms = get_waveforms(interloc_results, new_cat)
    waveforms['variable_length'] = variable_length_wf

    logger.info('event classification')

    station_context = waveforms['context'][0].stats.station

    context_2s = waveforms['fixed_length'].select(station=station_context)

    # clean the context trace

    composite = np.zeros(len(context_2s[0].data))
    vars = []

    for i, tr in enumerate(context_2s):
        context_2s[i].data = np.nan_to_num(context_2s[i].data)
        vars.append(np.var(context_2s[i].data))
        composite += context_2s[i].data ** 2

    index = np.argmax(vars)

    fixed_length = waveforms['fixed_length']
    context = waveforms['context']

    quick_magnitude_processor = quick_magnitude.Processor()
    result = quick_magnitude_processor.process(stream=fixed_length,
                                               cat=new_cat)
    new_cat = quick_magnitude_processor.output_catalog(new_cat)

    logger.info('calculating rays')
    rt_start_time = time()
    rtp = ray_tracer.Processor()
    rtp.process(cat=new_cat)
    new_cat = rtp.output_catalog(new_cat)
    rt_end_time = time()
    rt_processing_time = rt_end_time - rt_start_time
    logger.info(f'done calculating rays in {rt_processing_time} seconds')

    new_cat, send_automatic, send_api = event_classification(new_cat,
                                                             fixed_length,
                                                             context,
                                                             event_types_lookup)

    dict_out = waveforms
    dict_out['catalogue'] = new_cat

    set_event(event_id, **dict_out)

    if send_api or force_send_to_api:
        result = api_job_queue.submit_task(
            send_to_api,
            event_id=event_id,
            network=network_code,
            send_to_bus=send_automatic or force_send_to_automatic,
        )
        logger.info('event save to the API')

    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time
    record_processing_logs_pg(new_cat[0], 'success', __processing_step__,
                              __processing_step_id__, processing_time)

    logger.info('pre processing completed')

    return result
