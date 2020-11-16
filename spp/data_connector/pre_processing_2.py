"""
if the waveform extraction fails, resend to the queue!
"""

from datetime import datetime, timedelta
from time import time

import numpy as np
import requests
from pytz import utc

from microquake.helpers.logging import logger
from microquake.core.event import ResourceIdentifier
from microquake.core.stream import Stream
from spp.clients.ims import web_client
from spp.clients.api_client import (post_data_from_objects,
                                           get_event_by_id,
                                           put_data_from_objects,
                                           get_event_types)

from microquake.ml.signal_noise_classifier import SignalNoiseClassifier
from obspy import UTCDateTime
from microquake.core.settings import settings
from microquake.db.connectors import RedisQueue
    # record_processing_logs_pg
from microquake.db.models.redis import set_event, get_event
from microquake.processors import (clean_data, event_classifier, interloc,
                                   quick_magnitude, ray_tracer)
from microquake.pipelines.automatic_pipeline import automatic_pipeline
from microquake.helpers.timescale_db import (get_continuous_data,
                                                  get_db_lag)
from microquake.ml import signal_noise_classifier
from microquake.helpers.time import get_time_zone
from datetime import datetime
import json
import requests
from urllib.parse import urlencode

import seismic_client
from seismic_client.rest import ApiException


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
ims_base_url = settings.get('ims_base_url')
api_base_url = settings.get('api_base_url')
inventory = settings.inventory
network_code = settings.NETWORK_CODE

use_time_scale = settings.USE_TIMESCALE

# tolerance for how many trace are not recovered
minimum_recovery_fraction = settings.get(
    'data_connector').minimum_recovery_fraction


def event_within_tolerance(event_time, tolerance=0.5):
    """
    Return true if there is an event in the catalog within <tolerance>
    seconds of the event time
    :param event_time:
    :param tolerance:
    :return:
    """

    if isinstance(event_time, datetime):
        event_time = UTCDateTime(event_time)

    start_time = event_time - 0.25
    end_time = event_time + 0.25

    query = urlencode({'time_utc_after': start_time,
                       'time_utc_before': end_time})

    url = f'{api_base_url}events?{query}'
    # from pdb import set_trace; set_trace()
    response = json.loads(requests.get(url).content)

    if response['count']:
        return True

    return False


def send_to_api(cat, fixed_length, variable_length=None, context=None,
                attempt_number=1, send_to_bus=True, **kwargs):

    configuration = seismic_client.Configuration()
    configuration.username = 'data_connector'
    configuration.password = 'H5Bm7oeJ9xbX'

    logger.info('lolita')
    start_processing_time = time()

    event_resource_id = cat[0].resource_id.id

    if get_event_by_id(api_base_url, event_resource_id):
        logger.warning(f'event with id {event_resource_id} already exists '
                       f'in the database... the event '
                       f'will not be uploaded... exiting')
        return

    if attempt_number > 5:
        logger.warning('maximum number of attempt (5) reached. The event '
                       'will not be posted to the API')
        return

    else:
        attempt_number += 1

    response = None

    # if event_within_tolerance(cat[0].preferred_origin().time):
    #     return

    try:
        response = post_data_from_objects(api_base_url,
                                          network_code, event_id=None,
                                          cat=cat,
                                          stream=fixed_length,
                                          context=context,
                                          variable_length=variable_length,
                                          tolerance=None,
                                          send_to_bus=send_to_bus)

    except requests.exceptions.RequestException as err:
        logger.error(err)
        logger.info('request failed, resending to queue')
        result = api_job_queue.submit_task(send_to_api, cat, fixed_length,
                                           variable_length=variable_length,
                                           context=context,
                                           send_to_bus=send_to_bus,
                                           attempt_number=attempt_number)

    if not response:
        logger.info('request failed')
        logger.info(response.content)

    logger.info('request successful')
    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time

    logger.info(f'sent to the API in {processing_time}')

    return response



from microquake.db.connectors import RedisQueue
from microquake.processors import event_detection
# from obspy.signal.trigger import recursive_sta_lta, trigger_onset
from pytz import utc
from microquake.core.settings import settings
from spp.clients.ims import web_client
import pandas as pd
from microquake.core import Stream
from microquake.helpers import time as tme

evp = event_detection.Processor()
# extract_continuous_queue = settings.get('EXTRACT_CONTINUOUS_QUEUE')

# rq = RedisQueue(extract_continuous_queue)


# rq_api = RedisQueue('')

def event_classification(cat, mag, fixed_length, context, event_types_lookup):

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
    blast_event_types = settings.get(
        'event_classifier').blast_event_types.to_list()
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

    elif event_type in blast_event_types:
        cat[0].event_type = event_types_lookup[event_type]
        cat[0].preferred_origin().evaluation_status = 'rejected'

        logger.info(f'event categorized as {event_type}. The event will be '
                    f'marked as rejected but uploaded to the API. The event '
                    f'will be further processed.')
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

    # superseding the above if the magnitude of the event is greater than
    # 0. Unless the event is categorized as a blast, it will automatically
    # be categorized as a "genuine" seismic event, automatically processed
    # and saved.
    mag_threshold = settings.get(
        'event_classifier').large_event_processing_threshold
    ignored_types = settings.get('event_classifier').ignore_type_large_event
    if (mag > mag_threshold) and (event_type not in ignored_types):
        logger.info('superseding the classifier categorization, the event '
                    'will be classified as a seismic event and further '
                    'processed.')
        cat[0].event_type = event_types_lookup['seismic event']
        cat[0].preferred_origin().evaluation_status = 'preliminary'
        automatic_processing = True
        save_event = True

    return cat, automatic_processing, save_event


def get_context_trace(cat, st):
    utc_time = cat[0].preferred_origin().time
    start_time = utc_time - 10
    end_time = utc_time + 10

    ev_loc = cat[0].preferred_origin().loc

    dists = []
    sensors = []
    for station in inventory.stations():
        # we need to be careful here. This is very particular to Oyu Tolgoi
        if station.motion != 'VELOCITY':
            continue
        if len(st.select(station=station.code)) == 0:
            continue
        station_data = st.select(station=station.code)[0].data
        sampling_rate = st.select(station=station.code)[
            0].stats.sampling_rate
        dists.append(np.linalg.norm(ev_loc - station.loc))
        sensors.append(station.code)

    indices = np.argsort(dists)

    context_trace_filter = settings.get('data_connector').context_trace.filter

    for i in indices:
        if sensors[i] in settings.get('sensors').black_list:
            continue
        if not inventory.select(sensors[i]):
            continue
        if inventory.select(sensors[i]).motion == 'ACCELERATION':
            continue

        logger.info(f'getting context trace for station {sensors[i]} located '
                    f'at {dists[i]} m from the event hypocenter')

        context = web_client.get_continuous(ims_base_url, start_time, end_time,
                                            [sensors[i]], network=network_code)

        if not context:
            continue

        context.filter('bandpass', **context_trace_filter)

        if np.any(np.isnan(context.composite()[0].data)):
            continue

        if context:
            break
        else:
            logger.warning('context trace malformed continuing')

    return context


def run_interloc(st, cat):

    wf = st.copy()

    wf = wf.detrend('demean').taper(max_length=0.01, max_percentage=0.01)

    clean_data_processor = clean_data.Processor()
    clean_wf = clean_data_processor.process(waveform=wf.copy())

    max_len = np.max([len(tr) for tr in clean_wf])
    trs = [tr for tr in clean_wf if len(tr) == max_len]
    clean_wf.traces = trs

    interloc_processor = interloc.Processor()
    results = interloc_processor.process(stream=clean_wf)
    new_cat = interloc_processor.output_catalog(cat)

    logger.info('Event location: {}'.format(new_cat[0].preferred_origin().loc))
    logger.info('Event time: {}'.format(new_cat[0].preferred_origin().time))

    return new_cat.copy()


def extract_continuous_triggering(sensor_code, start_time, end_time, *args,
                                  **kwargs):
    st = extract_continuous(sensor_code, start_time, end_time, *args,
                            **kwargs)

    if st is None:
        return

    st = st.detrend('demean').detrend('linear')
    tr = st.copy().composite()[0]

    triggers = triggering(tr)

    return st, triggers


def extract_continuous(sensor_code, start_time, end_time, *args, **kwargs):

    print(start_time)
    print(end_time)

    logger.info(f'getting trace for station {sensor_code}')

    st = web_client.get_continuous(ims_base_url, start_time, end_time,
                                   [sensor_code], network=network_code)

    if len(st) == 0:
        return None

    st = st.detrend('linear').detrend('demean')

    return st


def triggering(trace):
    triggers_on, triggers_off = evp.process(trace=trace)

    if not triggers_on:
        return

    sensor = trace.stats.station

    triggers_out = []
    for trigger_on, trigger_off in zip(triggers_on, triggers_off):
        triggers_out.append([sensor, trigger_on, trigger_off])

    return triggers_out


def associate(triggers, window_size=2, hold_off=0, minimum_number_trigger=10):
    df = pd.DataFrame(triggers)

    cts = []
    for t in df['trigger_time']:
        df_tmp = df[(df['trigger_time'] >= t) & (df['trigger_time'] < t + 1)]
        cts.append(len(np.unique(df_tmp['sensor'])))

    df['count'] = cts
    df = df.sort_values('count', ascending=False)

    is_finished = False
    trigger_times = []
    while is_finished is False:
        if df.iloc[0]['count'] > minimum_number_trigger:
            t = df.iloc[0]['trigger_time']
            trigger_times.append(t)
            df = df[(df['trigger_time'] < t - 0.5) |
                    (df['trigger_time'] > t + 1.5)]

            cts = []
            for t in df['trigger_time']:
                df_tmp = df[
                    (df['trigger_time'] >= t) & (df['trigger_time'] < t + 1)]
                cts.append(len(np.unique(df_tmp['sensor'])))

            df['count'] = cts
            df = df.sort_values('count', ascending=False)

            if len(df) == 0:
                is_finished = True

        else:
            is_finished = True

    return trigger_times


def rq_map(function, iterator, queue, *args, execution_time_out=20, **kwargs):
    """

    :param function: function to apply to the iterator
    :param iterator: an iterator
    :param queue: the queue name
    :param execution_time_out: The time allowed for a job to finish once it is
    started, this does not include the queued time.
    :return:
    """

    rq = RedisQueue(queue)

    jobs = []
    for it in iterator:
        jobs.append(rq.submit_task(function, it, *args, **kwargs))

    # waiting for the processing to complete
    t0 = time()
    time_since_started = np.zeros(len(jobs))
    completed = [False] * len(jobs)

    time_out = execution_time_out

    while not np.all(completed):
        is_started = [job.is_started for job in jobs]
        for i, (job, tss) in enumerate(zip(jobs, time_since_started)):
            if job.is_finished:
                completed[i] = True
            elif job.is_started and (tss == 0):
                time_since_started[i] = time()
            elif job.is_failed:
                completed[i] = True
            elif not job.is_started:
                pass
            elif time() - tss > time_out:
                completed[i] = True
        # print(completed)

        progress = int(np.count_nonzero(completed) / len(completed) * 100)
        elapsed_time = time() - t0
        if progress == 0:
            eta = 100
        else:
            eta = elapsed_time / progress * (100 - progress)
        print('\r[{0:50s}] {1}% -- ETA {2:0.1f} -- Elapsed time {3:0.1f} '
              'seconds'
              .format('#' * int((progress / 100) * 50), progress, eta,
                      elapsed_time),
              end='\r', flush=True)

    return [job.result if not job.is_failed else None for job in jobs]


def process_individual_event(input_dict, *args, force_send_to_api=False,
                             force_send_to_automatic=False,
                             force_accept=False, **kwargs):

    start_processing_time = time()
    st = input_dict['stream']
    cat = input_dict['cat']

    event_id = cat[0].resource_id.id

    waveforms = {'fixed_length': st}

    wf = st.copy().detrend('demean').taper(max_length=0.01,
                                           max_percentage=0.01)

    clean_data_processor = clean_data.Processor()
    clean_wf = clean_data_processor.process(waveform=wf.copy())

    max_len = np.max([len(tr) for tr in clean_wf])
    trs = [tr for tr in clean_wf if len(tr) == max_len]
    clean_wf.traces = trs

    interloc_processor = interloc.Processor()
    results = interloc_processor.process(stream=clean_wf)
    cat_interloc = interloc_processor.output_catalog(cat.copy())

    cat_interloc[0].preferred_origin().evaluation_status = 'preliminary'

    context = get_context_trace(cat_interloc, st)
    waveforms['context'] = context

    try:
        event_types_lookup = get_event_types(api_base_url)
    except ConnectionError:
        logger.error('api connection error')
        # set_event(event_id, **event)
        # we_job_queue.submit_task(pre_process, event_id=event_id)

    try:
        variable_length_wf = web_client.get_seismogram_event(ims_base_url, cat[0],
                                                             network_code, utc)
    except AttributeError as ae:
        logger.warning('could not retrieve the variable length waveforms')
        logger.error(ae)
        variable_length_wf = None
    except Exception as exception:
        logger.error(exception)
        variable_length_wf = None

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
                                                   cat=cat_interloc.copy())
    new_cat = quick_magnitude_processor.output_catalog(cat_interloc.copy())

    snc = SignalNoiseClassifier()

    tr = fixed_length.select(station=context[0].stats.station)
    classes = snc.predict(tr, context, new_cat.copy())
    logger.info(classes)
    if classes['signal'] < 0.1:
        logger.info('event identified as noise...')

        if not force_send_to_api or force_accept:
            logger.info('the event will not be further processed')
            logger.info('exiting')
            return

        if force_send_to_api:
            logger.info('the event will be sent to the API')
        if force_accept:
            logger.info('the event will be shown as accepted')

    new_cat, send_automatic, send_api = event_classification(new_cat.copy(),
                                                             qmag,
                                                             fixed_length,
                                                             context,
                                                             event_types_lookup)

    logger.info('all event that pass this point will be sent to the API')

    send_api = True

    if force_send_to_api or send_api:
        logger.info('calculating rays')
        rt_start_time = time()

        input_dicts = []
        for station in inventory.stations():
            st_loc = station.loc
            station_code = station.code
            for phase in ['P', 'S']:
                input_dict = {'station_code': station_code,
                              'phase': phase}
                input_dicts.append(input_dict)

        ev_loc = new_cat[0].preferred_origin().loc
        p_ori = new_cat[0].preferred_origin()
        rays = rq_map(ray_tracing, input_dicts, 'test', ev_loc, p_ori.copy())
        for ray in rays:
            if ray is None:
                continue
            new_cat[0].preferred_origin().append_ray(ray)

        rt_end_time = time()
        rt_processing_time = rt_end_time - rt_start_time
        logger.info(f'done calculating rays in {rt_processing_time} seconds')

    if force_accept:
        new_cat[0].preferred_origin().evaluation_status = 'preliminary'

    dict_out = waveforms
    dict_out['catalogue'] = new_cat

    set_event(event_id, **dict_out)

    rq = RedisQueue('test')
    if send_api or force_send_to_api:
        logger.info('event will be saved!')
        send_to_api(new_cat, st, variable_length=variable_length_wf,
                    context=context, attempt_number=1, send_to_bus=True)
        # result = rq.submit_task(
        #     send_to_api, fixed_length,
        #     variable_length=variable_length_wf,
        #     context=context,
        #     network=network_code,
        #     send_to_bus=send_automatic or force_send_to_automatic,
        # )
        logger.info('event will be saved to the API')

    # if send_api or force_send_to_api:
    #     result = api_job_queue.submit_task(
    #         send_to_api,
    #         event_id=event_id,
    #         network=network_code,
    #         send_to_bus=send_automatic or force_send_to_automatic,
    #     )
    #     logger.info('event will be saved to the API')

    end_processing_time = time()
    processing_time = end_processing_time - start_processing_time
    # record_processing_logs_pg(new_cat[0], 'success', __processing_step__,
    #                           __processing_step_id__, processing_time)

    logger.info(f'pre processing completed in {processing_time} s')


def ray_tracing(input_dict, ev_loc, p_ori):
    from microquake.helpers.grid import get_ray, get_grid_point

    phase = input_dict['phase']
    station_code = input_dict['station_code']

    logger.info(f'calculating ray for {phase}-wave for '
                f'station {station_code}')
    try:
        ray = get_ray(station_code, phase, ev_loc)
        ray.station_code = station_code
        ray.phase = phase
        ray.arrival_id = p_ori.get_arrival_id(phase, station_code)
        ray.travel_time = get_grid_point(station_code, phase,
                                         ev_loc, grid_type='time')
        ray.azimuth = get_grid_point(station_code, phase,
                                     ev_loc, grid_type='azimuth')
        ray.takeoff_angle = get_grid_point(station_code, phase,
                                           ev_loc,
                                           grid_type='take_off')
    except FileNotFoundError:
        logger.warning(f'travel time grid for station '
                       f'{station_code} '
                       f'and phase {phase} was not found. '
                       f'Skipping...')
        ray = None

    return ray


def extract_data_events(event_time, cat):

    start_time = event_time - 3
    end_time = event_time + 3

    sensors = []
    for sensor in inventory.stations():
        if sensor.code in settings.get('sensors').black_list:
            continue
        sensors.append(sensor.code)

    logger.info('Extracting seismogram and triggering')

    results = rq_map(extract_continuous_triggering, sensors, 'test', start_time,
                     end_time, execution_time_out=20)

    trs = []
    triggers = {'sensor': [],
                'trigger_time': []}
    for result in results:
        if not result:
            continue
        for tr in result[0]:
            trs.append(tr)

        if not result[1]:
            continue
        for trigger in result[1]:
            triggers['sensor'].append(trigger[0])
            triggers['trigger_time'].append(trigger[1])

    st = Stream(traces=trs)

    association_times = associate(triggers)

    if not association_times:
        associated_times = [event_time]

    sts = []
    cats = []
    event_id_prefix = ''
    for i, at in enumerate(association_times):
        if (at - start_time < 0.5) or (end_time - at < 1.5):
            stime = at - 0.5
            etime = at + 1.5
            tmp = rq_map(extract_continuous, sensors, 'test', stime, etime)

        if event_id_prefix == '':
            event_id_prefix = 'a'
        else:
            event_id_prefix = chr(ord(event_id_prefix) + 1)

        print(cat[0].resource_id)
        cat[0].resource_id = ResourceIdentifier()
        for j, ori in enumerate(cat[0].origins):
            cat[0].origins[j].resource_id = ResourceIdentifier()
        for j, mag in enumerate(cat[0].magnitudes):
            cat[0].magnitudes[j].resource_id = ResourceIdentifier()
        cat[0].preferred_origin().time = at
        cats.append(cat.copy())
        sts.append(st.copy().trim(starttime=at-0.5, endtime=at+1.5))

    logger.info('Interloc')

    sts_cats = []
    for c, s in zip(cats, sts):
        st_cat = {'stream': s.copy(),
                  'cat': c.copy()}
        sts_cats.append(st_cat)
        # rq.submit_task(process_individual_event, st_cat,
        #                force_send_to_api=True)

        process_individual_event(st_cat, force_send_to_api=True)

    results = rq_map(process_individual_event, sts_cats, 'test',
                     execution_time_out=600, force_send_to_api=True)

    logger.info('extracting')


def pre_process(event_id, force_send_to_api=False, force_accept=False,
                force_send_to_automatic=False):
    event = get_event(event_id)
    cat = event['catalogue']
    event_time = cat[0].preferred_origin().time
    start_time = event_time - 3

    end_time = event_time + 3

    sensors = []
    for sensor in inventory.stations():
        if sensor.code in settings.get('sensors').black_list:
            continue
        sensors.append(sensor.code)

    logger.info('Extracting seismogram and triggering')

    results = rq_map(extract_continuous_triggering, sensors, 'test',
                     start_time,
                     end_time, execution_time_out=20)

    trs = []
    triggers = {'sensor': [],
                'trigger_time': []}
    for result in results:
        if not result:
            continue
        for tr in result[0]:
            trs.append(tr)

        if not result[1]:
            continue
        for trigger in result[1]:
            triggers['sensor'].append(trigger[0])
            triggers['trigger_time'].append(trigger[1])

    st = Stream(traces=trs)

    association_times = associate(triggers)

    if not association_times:
        associated_times = [event_time]

    sts = []
    cats = []
    event_id_prefix = ''
    for i, at in enumerate(association_times):
        if (at - start_time < 0.5) or (end_time - at < 1.5):
            stime = at - 0.5
            etime = at + 1.5
            tmp = rq_map(extract_continuous, sensors, 'test', stime, etime)

        if event_id_prefix == '':
            event_id_prefix = 'a'
        else:
            event_id_prefix = chr(ord(event_id_prefix) + 1)

        # cat[0].resource_id.id = event_id_prefix + cat[0].resource_id.id
        print(cat[0].resource_id)
        cat[0].resource_id = ResourceIdentifier()
        for j, ori in enumerate(cat[0].origins):
            cat[0].origins[j].resource_id = ResourceIdentifier()
        for j, mag in enumerate(cat[0].magnitudes):
            cat[0].magnitudes[j].resource_id = ResourceIdentifier()
        cat[0].preferred_origin().time = at
        cats.append(cat.copy())
        sts.append(st.copy().trim(starttime=at - 0.5, endtime=at + 1.5))

    sts_cats = []
    for c, s in zip(cats, sts):
        st_cat = {'stream': s.copy(),
                  'cat': c.copy()}
        sts_cats.append(st_cat)
        # rq.submit_task(process_individual_event, st_cat,
        #                force_send_to_api=True)

        process_individual_event(st_cat,
                                 force_send_to_api=force_send_to_api,
                                 force_accept=force_accept,
                                 force_send_to_automatic=
                                 force_send_to_automatic)

    # results = rq_map(process_individual_event, sts_cats, 'test',
    #                  execution_time_out=600, force_send_to_api=True)

    logger.info('extracting')


def test():

    from microquake.db.connectors import RedisQueue
    from microquake.processors import event_detection
    from obspy.core import UTCDateTime
    from time import time
    from microquake.core.settings import settings
    from time import time
    from microquake.helpers import time as tme
    from spp.clients.ims import web_client
    from microquake.helpers.logging import logger


    event_time = UTCDateTime(2020, 7, 10, 14, 56, 14)
    # event_time = UTCDateTime(2020, 7, 10, 20, 46, 16)
    start_time = event_time - 3

    rq = RedisQueue('test')
    end_time = event_time + 3
    jobs = []

    tz = tme.get_time_zone()

    logger.info('retrieving catalog from the IMS system')
    cat = web_client.get_catalogue(ims_base_url, start_time, end_time,
                                   inventory, tz, accepted=False)
    logger.info('done retrieving the catalog from the IMS system')
    logger.info(f'the catalog length is {len(cat)}')

    sensors = []
    for sensor in inventory.stations():
        if sensor.code in settings.get('sensors').black_list:
            continue
        sensors.append(sensor.code)

    logger.info('Extracting seismogram and triggering')

    results = rq_map(extract_continuous_triggering, sensors, 'test', start_time,
                     end_time, execution_time_out=20)

    trs = []
    triggers = {'sensor': [],
                'trigger_time': []}
    for result in results:
        if not result:
            continue
        for tr in result[0]:
            trs.append(tr)

        if not result[1]:
            continue
        for trigger in result[1]:
            triggers['sensor'].append(trigger[0])
            triggers['trigger_time'].append(trigger[1])

    st = Stream(traces=trs)

    association_times = associate(triggers)

    if not association_times:
        associated_times = [event_time]

    sts = []
    cats = []
    event_id_prefix = ''
    for i, at in enumerate(association_times):
        if (at - start_time < 0.5) or (end_time - at < 1.5):
            stime = at - 0.5
            etime = at + 1.5
            tmp = rq_map(extract_continuous, sensors, 'test', stime, etime)

        if event_id_prefix == '':
            event_id_prefix = 'a'
        else:
            event_id_prefix = chr(ord(event_id_prefix) + 1)

        # cat[0].resource_id.id = event_id_prefix + cat[0].resource_id.id
        print(cat[0].resource_id)
        cat[0].resource_id = ResourceIdentifier()
        for j, ori in enumerate(cat[0].origins):
            cat[0].origins[j].resource_id = ResourceIdentifier()
        for j, mag in enumerate(cat[0].magnitudes):
            cat[0].magnitudes[j].resource_id = ResourceIdentifier()
        cat[0].preferred_origin().time = at
        cats.append(cat.copy())
        sts.append(st.copy().trim(starttime=at-0.5, endtime=at+1.5))

    logger.info('Interloc')

    sts_cats = []
    for c, s in zip(cats, sts):
        st_cat = {'stream': s.copy(),
                  'cat': c.copy()}
        sts_cats.append(st_cat)
        # rq.submit_task(process_individual_event, st_cat,
        #                force_send_to_api=True)

        process_individual_event(st_cat, force_send_to_api=True)

    # results = rq_map(process_individual_event, sts_cats, 'test',
    #                  execution_time_out=600, force_send_to_api=True)

    logger.info('extracting')

    return results


# def test_signal_noise_classifier():
#     from microquake.helpers import time as tme
#
#     start_time = UTCDateTime(2020, 7, 5, 8, 51, 13)
#     end_time = UTCDateTime.now()
#
#     tz = tme.get_time_zone()
#
#     cat = web_client.get_catalogue(ims_base_url, start_time, end_time,
#                                    inventory, tz)
#
#     for event in cat:
