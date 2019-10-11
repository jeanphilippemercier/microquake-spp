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
from microquake.core import Stream
from obspy import UTCDateTime
from microquake.core.settings import settings
from microquake.db.connectors import RedisQueue, record_processing_logs_pg
from microquake.db.models.redis import set_event, get_event
from microquake.pipelines.automatic_pipeline import automatic_pipeline
from microquake.processors import (clean_data, event_classifier, interloc,
                                   quick_magnitude, ray_tracer)
from microquake.core.helpers.timescale_db import get_continuous_data

automatic_message_queue = settings.AUTOMATIC_PIPELINE_MESSAGE_QUEUE
automatic_job_queue = RedisQueue(automatic_message_queue)
api_message_queue = settings.API_MESSAGE_QUEUE
api_job_queue = RedisQueue(api_message_queue)

__processing_step__ = 'pre_processing'
__processing_step_id__ = 2

sites = [int(station.code) for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')
inventory = settings.inventory
network_code = settings.NETWORK_CODE

# tolerance for how many trace are not recovered
minimum_recovery_fraction = settings.get(
    'data_connector').minimum_recovery_fraction


def extract_continuous(starttime, endtime, sensor_id=None):
    st = get_continuous_data(starttime, endtime, sensor_id)

    if sensor_id is not None:
        sensors = [sensor_id]
    else:
        sensors = sites

    if st is None:
        logger.warning('request of the continuous data from the '
                       'TimescaleDB returned None... requesting data from '
                       'the IMS system through the web API instead!')

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

        logger.info('getting context trace for station {}'.format(stations[i]))

        context = web_client.get_continuous(base_url, starttime, endtime,
                                            stations[i], utc,
                                            network=network_code)

        context.filter('bandpass', **context_trace_filter)

        if context:
            break

    waveforms = {'fixed_length': fixed_length_wf,
                 'context': context}

    return waveforms


def send_to_api(event_id, **kwargs):

    processing_step = 'post_event_api'
    processing_step_id = 4
    start_processing_time = time()

    api_base_url = settings.get('api_base_url')
    event = get_event(event_id)

    if event is None:
        logger.error(f'The event {event_id} is not available anymore, exiting')
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
                                          send_to_bus=False)
    except requests.exceptions.ConnectionError as e:
        logger.error(e)
        logger.info('request failed, resending to queue')

        set_event(event_id, **event)

        result = api_job_queue.submit_task(send_to_api, event_id=event_id)

    if not response:
        logger.info('request failed, resending to the queue')
        set_event(event_id, **event)
        result = api_job_queue.submit_task(send_to_api, event_id=event_id)

    logger.info('request successful')
    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time

    evt = event['catalogue'][0]

    record_processing_logs_pg(evt, 'success', processing_step,
                              processing_step_id, processing_time)

    return response


def pre_process(event_id, **kwargs):

    start_processing_time = time()

    logger.info('message received')

    # tmp = deserialize(message)
    start_processing_time = time()
    event = get_event(event_id)
    cat = event['catalogue']

    event_time = cat[0].preferred_origin().time.datetime.timestamp()

    closing_window_time_seconds = settings.get(
        'data_connector').closing_window_time_seconds

    # if UTCDateTime.now() - event_time < closing_window_time_seconds:
    #     logger.info('Delay between the detection of the event and the '
    #                 'current time ({} s) is smaller than the closing window '
    #                 'time threshold ({} s). The event will be resent to the '
    #                 'queue and reprocessed at a later time '.format(
    #             UTCDateTime.now() - event_time, closing_window_time_seconds))
    #
    #     return

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

    category = event_classifier.Processor().process(stream=fixed_length,
                                                    context=context,
                                                    cat=new_cat)

    sorted_list = sorted(category.items(), reverse=True,
                         key=lambda x: x[1])

    elevation = new_cat[0].preferred_origin().z
    maximum_event_elevation = settings.get(
        'data_connector').maximum_event_elevation

    logger.info('{}'.format(sorted_list))

    event_type = sorted_list[0][0]

    likelihood_threshold = settings.get('data_connector').likelihood_threshold

    if event_type in ['anthropogenic event', 'other event']:
        if sorted_list[1][1] < likelihood_threshold / 2:
            logger.info('event categorized as noise are not further processed '
                        'and will not be saved in the database')
            end_processing_time = time()
            processing_time = end_processing_time - start_processing_time
            record_processing_logs_pg(new_cat[0], 'success',
                                      __processing_step__,
                                      __processing_step_id__, processing_time)
            return

        elif elevation < maximum_event_elevation:
            new_cat[0].event_type = event_type
            new_cat[0].preferred_origin().evaluation_status = 'preliminary'
            logger.info('event categorized noise but could also be {} with '
                        'a likelihood of {}.'.format(sorted_list[1][0],
                                                     sorted_list[1][1]))
            logger.info('The event will be kept and further processed')

    elif sorted_list[0][1] > likelihood_threshold and elevation < \
            maximum_event_elevation:
        new_cat[0].event_type = event_type
        new_cat[0].preferred_origin().evaluation_status = 'preliminary'

        logger.info('event categorized as {} with a likelihood of {}'
                    ''.format(sorted_list[0][0], sorted_list[0][1]))

    else:
        logger.info('event categorized as noise are not further processed '
                    'and will not be saved in the database')

        end_processing_time = time()
        processing_time = end_processing_time - start_processing_time
        record_processing_logs_pg(new_cat[0], 'success', __processing_step__,
                                  __processing_step_id__, processing_time)

        return

    dict_out = waveforms
    dict_out['catalogue'] = new_cat

    set_event(event_id, **dict_out)

    result = api_job_queue.submit_task(send_to_api, event_id=event_id)

    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time
    record_processing_logs_pg(new_cat[0], 'success', __processing_step__,
                              __processing_step_id__, processing_time)

    logger.info('sending to automatic pipeline')

    result = automatic_job_queue.submit_task(automatic_pipeline,
                                             event_id=event_id)

    logger.info('done collecting the waveform, happy processing!')

    return result
