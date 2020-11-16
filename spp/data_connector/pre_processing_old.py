"""
if the waveform extraction fails, resend to the queue!
"""

from datetime import datetime, timedelta
from time import time

import numpy as np
import requests
from pytz import utc

from spp.core.helpers.logging import logger
from microquake.core.stream import Stream
from spp.clients.ims import web_client
from spp.clients.api_client import (SeismicClient, post_data_from_objects,
                                    get_event_by_id, put_data_from_objects,
                                    get_event_types)
from obspy import UTCDateTime
from microquake.core.settings import settings
from spp.db.connectors import RedisQueue
    # record_processing_logs_pg
from spp.db.models.redis import set_event, get_event
from spp.processors import (clean_data, interloc,
                                   quick_magnitude, ray_tracer)
from microquake.core.helpers.timescale_db import (get_continuous_data,
                                                  get_db_lag)
from microquake.core.helpers.time import get_time_zone
from microquake.ml.signal_noise_classifier import SignalNoiseClassifier
from microquake.ml.classifier import EventClassifier
from datetime import datetime
import json
import requests
from urllib.parse import urlencode

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

use_time_scale = settings.USE_TIMESCALE

# tolerance for how many trace are not recovered
minimum_recovery_fraction = settings.get(
    'data_connector').minimum_recovery_fraction

api_user = settings.get('api_user')
api_password = settings.get('api_password')
sc = SeismicClient(api_base_url, api_user, api_password)


def event_within_tolerance(event_time, tolerance=0.5):
    """
    Return true if there is an event in the catalog within <tolerance>
    seconds of the event time
    :param event_time:
    :param tolerance:
    :return:
    """

    if tolerance is None:
        return False

    api_base_url = settings.get('api_base_url')

    if isinstance(event_time, datetime):
        event_time = UTCDateTime(event_time)

    start_time = event_time - tolerance
    end_time = event_time + tolerance

    query = urlencode({'time_utc_after': start_time,
                       'time_utc_before': end_time})

    if api_base_url[-1] != '/':
        api_base_url += '/'

    url = f'{api_base_url}events?{query}'
    # from pdb import set_trace; set_trace()
    response, res = sc.events_list(start_time=start_time, end_time=end_time)
    # response = json.loads(requests.get(url, auth=).content)

    if len(res):
        logger.info(f'event found within {tolerance} second of the current'
                    f' event')
        return True

    return False


def extract_continuous(starttime, endtime, sensor_id=None):
    s_time = time()
    trs = []

    if type(starttime) is datetime:
        starttime = UTCDateTime(starttime)
    if type(endtime) is datetime:
        endtime = UTCDateTime(endtime)

    duration = endtime - starttime

    st = None
    if use_time_scale:
        st = get_continuous_data(starttime, endtime, sensor_id=sensor_id)
    else:
        sensors = [station.code for station in inventory.stations()]
        st = web_client.get_continuous(base_url, starttime, endtime,
                                       sensors, network=network_code)

        return st

    if st is not None:
        for tr in st:
            trs.append(tr)

    if sensor_id is not None:
        if st is None:
            st = web_client.get_continuous(base_url, starttime, endtime,
                                           [str(sensor_id)],
                                           network=network_code)

            if st is None:
                return None

            expected_number_sample = duration * st[0].stats.sampling_rate
            if len(st[0].data) < 0.9 * expected_number_sample:
                logger.warning(f'not enough data for sensor {sensor_id}')
                return None

        return st

    for sensor in inventory.stations():
        if sensor.code in st.unique_stations():
            continue

        logger.info(f'requesting data for sensor {sensor.code}')
        logger.info('no data in the timescale db, requesting from the '
                    'IMS system')
        st_tmp = web_client.get_continuous(base_url, starttime, endtime,
                                           [sensor.code],
                                           network=network_code)

        if (st_tmp is None) or (len(st_tmp) == 0):
            continue

        expected_number_sample = duration * st_tmp[0].stats.sampling_rate
        if len(st_tmp[0].data) < 0.9 * expected_number_sample:
            logger.warning(f'data only partially recovered for sensor'
                           f'{sensor}. The trace will not be kept')
            continue

        for tr in st_tmp:
            trs.append(tr)

    if not trs:
        return None

    return Stream(traces=trs)


def interloc_election(cat):

    event = cat[0]

    event_time = event.preferred_origin().time.datetime.replace(
        tzinfo=utc)

    interloc_results = []
    thresholds = []
    wfs = []

    starttime = event_time - timedelta(seconds=1.5)
    endtime = event_time + timedelta(seconds=3.5)

    complete_wf = extract_continuous(starttime, endtime)

    complete_wf.detrend('demean').taper(max_percentage=0.001,
                                        max_length=0.01).filter('bandpass',
                                                                freqmin=60,
                                                                freqmax=500)
    for offset in [-1.5, -0.5, 0.5, 1.5]:
        starttime = event_time + timedelta(seconds=offset)
        endtime = starttime + timedelta(seconds=2)

        wf = complete_wf.copy()
        wf.trim(starttime=UTCDateTime(starttime),
                endtime=UTCDateTime(endtime))
        wf = wf.detrend('demean').taper(max_length=0.01, max_percentage=0.01)

        clean_data_processor = clean_data.Processor()
        clean_wf = clean_data_processor.process(waveform=wf.copy())

        max_len = np.max([len(tr) for tr in clean_wf])
        trs = [tr for tr in clean_wf if len(tr) == max_len]
        clean_wf.traces = trs

        interloc_processor = interloc.Processor()
        results = interloc_processor.process(stream=clean_wf)
        new_cat = interloc_processor.output_catalog(cat)
        results['catalog'] = new_cat.copy()
        interloc_results.append(results)
        wfs.append(wf)
        thresholds.append(results['normed_vmax'])

    index = np.argmax(thresholds)

    cat_out = interloc_results[index]['catalog'].copy()

    logger.info('Event location: {}'.format(cat_out[0].preferred_origin().loc))
    logger.info('Event time: {}'.format(cat_out[0].preferred_origin().time))

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
        if not inventory.select(stations[i]):
            continue

        logger.info(f'getting context trace for station {stations[i]} located '
                    f'at {dists[i]} m from the event hypocenter')

        context = web_client.get_continuous(base_url, starttime, endtime,
                                            [stations[i]],
                                            network=network_code)

        if not context:
            continue

        context.filter('bandpass', **context_trace_filter)

        if np.any(np.isnan(context.composite()[0].data)):
            continue

        if context:
            break
        else:
            logger.warning('context trace malformed continuing')

    waveforms = {'fixed_length': fixed_length_wf,
                 'context': context}

    return waveforms


def send_to_api(event_id, tolerance=0.5, **kwargs):

    start_processing_time = time()
    send_to_bus = kwargs.get('send_to_bus', True)

    event = get_event(event_id)

    cat = event['catalogue']
    fixed_length = event['fixed_length']

    event_resource_id = cat[0].resource_id.id

    response, event_api = sc.events_read(event_resource_id)

    if event_api:
        logger.warning('event already exists in the database... the event '
                       'will not be uploaded... exiting')
        return

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

    response = None

    if event_within_tolerance(event['catalogue'][0].preferred_origin().time,
                              tolerance=tolerance):
        logger.warning('event withing ')
        return

    try:
        response = sc.post_data_from_objects(network_code, event_id=None,
                                             cat=event['catalogue'],
                                             stream=event['fixed_length'],
                                             context=event['context'],
                                             variable_length=event['variable_length'],
                                             tolerance=tolerance,
                                             send_to_bus=send_to_bus)
        # response = post_data_from_objects(api_base_url,network_code, event_id=None,
        #                                   cat=event['catalogue'],
        #                                   stream=event['fixed_length'],
        #                                   context=event['context'],
        #                                   variable_length=event['variable_length'],
        #                                   tolerance=tolerance,
        #                                   send_to_bus=send_to_bus)
        #                                   network_code, event_id=None,
        #                                   cat=event['catalogue'],
        #                                   stream=event['fixed_length'],
        #                                   context=event['context'],
        #                                   variable_length=event['variable_length'],
        #                                   tolerance=tolerance,
        #                                   send_to_bus=send_to_bus)

    except requests.exceptions.RequestException as e:
        logger.error(e)
        return
        logger.info('request failed, resending to queue')
        set_event(event_id, **event)
        result = api_job_queue.submit_task(send_to_api, event_id=event_id,
                                           **kwargs)

    if not response:
        logger.info('request failed')
        # return
        # logger.info('request failed, resending to the queue')
        # set_event(event_id, **event)
        # result = api_job_queue.submit_task(send_to_api, event_id=event_id)

    logger.info('request successful')
    end_processing_time = time()

    # record_processing_logs_pg(evt, 'success', processing_step,
    #                           processing_step_id, processing_time)

    # if (cat[0].event_type == 'earthquake') or (cat[0].event_type ==
    #                                            'explosion'):
    #     logger.info('automatic processing')
    #     cat_auto = automatic_pipeline(cat=cat, stream=fixed_length)
    #
    #     put_data_from_objects(api_base_url, cat=cat_auto)

    return response


def event_classification(cat, mag, fixed_length, context, event_types_lookup):

    ec = EventClassifier()

    tr = fixed_length.select(station=context[0].stats.station)
    classes = ec.predict(tr, context, cat.copy())
    logger.info(classes)

    # category = event_classifier.Processor().process(stream=fixed_length,
    #                                                 context=context,
    #                                                 cat=cat)

    sorted_list = sorted(classes.items(), reverse=True,
                         key=lambda x: x[1])

    logger.info('{}'.format(sorted_list))

    # if classes['seismic event'] > 0.1:
    #     event_type = 'seismic event'
    #     likelihood = classes['seismic event']
    # else:
    event_type = sorted_list[0][0]
    likelihood = sorted_list[0][1]

    likelihood_threshold = settings.get(
        'event_classifier').likelihood_threshold
    accepted_event_types = settings.get(
        'event_classifier').valid_event_types.to_list()
    blast_event_types = settings.get(
        'event_classifier').blast_event_types.to_list()
    uploaded_event_types = settings.get(
        'event_classifier').uploaded_event_types.to_list()
    blast_window_starts = settings.get('event_classifier').blast_window_starts
    blast_window_ends = settings.get('event_classifier').blast_window_ends

    automatic_processing = False
    save_event = True

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

    if event_type in ['seismic event', 'unknown']:

        cat[0].event_type = event_types_lookup[event_type]
        # if likelihood >= likelihood_threshold:
        cat[0].preferred_origin().evaluation_status = 'preliminary'
        logger.info(f'event categorized as {event_type} will be further '
                    f'processed')
        automatic_processing = True

    elif event_type in blast_event_types:
        cat[0].event_type = event_types_lookup[event_type]
        cat[0].preferred_origin().evaluation_status = 'rejected'

        logger.info(f'event categorized as {event_type}. The event will be '
                    f'marked as rejected but uploaded to the API. The event '
                    f'will be further processed.')
        automatic_processing = True

    else:
        logger.info(f'event categorized as {event_type}. The event will '
                    f'be send to the API but will not be be further'
                    f' processed.')
        cat[0].event_type = event_types_lookup[event_type]
        cat[0].preferred_origin().evaluation_status = 'rejected'

    return cat, automatic_processing, save_event


def pre_process(event_id, force_send_to_api=False,
                force_send_to_automatic=False,
                force_accept=False, **kwargs):

    start_processing_time = time()

    logger.info('message received')

    # tmp = deserialize(message)
    start_processing_time = time()
    event = get_event(event_id)

    cat = event['catalogue']

    event_resource_id = cat[0].resource_id.id

    if get_event_by_id(api_base_url, event_resource_id):
        logger.warning('event already exists in the database... skipping!')
        return

    try:
        event_types_lookup = get_event_types(api_base_url)
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
    except Exception as err:
        logger.error(err)
        logger.error('resending to the queue')
        set_event(event_id, **event)
        we_job_queue.submit_task(pre_process, event_id=event_id)

    interloc_results = interloc_election(cat)

    if not interloc_results:
        logger.error('interloc failed')
        return

    new_cat = interloc_results['catalog'].copy()
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

    clean_data_processor = clean_data.Processor()
    fixed_length = clean_data_processor.process(waveform=waveforms[
        'fixed_length'].copy())
    context = waveforms['context']

    quick_magnitude_processor = quick_magnitude.Processor()
    qmag, _, _ = quick_magnitude_processor.process(stream=fixed_length,
                                                   cat=new_cat)
    new_cat = quick_magnitude_processor.output_catalog(new_cat)

    snc = SignalNoiseClassifier()

    tr = fixed_length.select(station=context[0].stats.station)
    classes = snc.predict(tr, context, new_cat.copy())
    logger.info(classes)
    if force_accept or force_send_to_api:
        classes['signal'] = 1
    if classes['signal'] < 0.1:
        logger.info('event identified as noise...')

        logger.info('the event will not be further processed')
        logger.info('exiting')
        return

    new_cat, send_automatic, send_api = event_classification(new_cat.copy(),
                                                             qmag,
                                                             fixed_length,
                                                             context,
                                                             event_types_lookup)

    logger.info('calculating rays')
    rt_start_time = time()
    rtp = ray_tracer.Processor()
    rtp.process(cat=new_cat)
    new_cat = rtp.output_catalog(new_cat)
    rt_end_time = time()
    rt_processing_time = rt_end_time - rt_start_time
    logger.info(f'done calculating rays in {rt_processing_time} seconds')

    if force_accept:
        new_cat[0].preferred_origin().evaluation_status = 'preliminary'

    dict_out = waveforms
    dict_out['catalogue'] = new_cat

    set_event(event_id, **dict_out)

    if send_api:
        result = api_job_queue.submit_task(
            send_to_api,
            event_id=event_id,
            network=network_code,
            send_to_bus=send_automatic or force_send_to_automatic,
        )
        logger.info('event will be saved to the API')

    if force_send_to_api:
        logger.info('force send to API')
        result = api_job_queue.submit_task(
            send_to_api,
            event_id=event_id,
            network=network_code,
            send_to_bus=send_automatic or force_send_to_automatic,
            tolerance=None
        )
        logger.info('event will be saved to the API')


    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time
    # record_processing_logs_pg(new_cat[0], 'success', __processing_step__,
    #                           __processing_step_id__, processing_time)

    logger.info(f'pre processing completed in {processing_time} s')

    return dict_out
