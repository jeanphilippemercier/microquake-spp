"""
if the waveform extraction fails, resend to the queue!
"""

from datetime import datetime, timedelta
from io import BytesIO

import msgpack
import numpy as np
from pytz import utc

from loguru import logger
from microquake.core import UTCDateTime, read_events
from microquake.IMS import web_client
from pymongo import MongoClient
from redis import Redis
from spp.core.settings import settings
from spp.pipeline import clean_data, event_classifier, interloc
from spp.utils.seismic_client import post_data_from_objects
from spp.core.connectors import connect_redis, connect_mongo

redis = connect_redis()
mongo_client = connect_mongo()
message_queue = settings.get('processing_flow').extract_waveforms.message_queue

sites = [int(station.code) for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')
inventory = settings.inventory
network_code = settings.NETWORK_CODE

# tolerance for how many trace are not recovered
minimum_recovery_fraction = settings.get(
    'data_connector').minimum_recovery_fraction


def interloc_election(cat):

    event = cat[0]

    event_time = event.preferred_origin().time.datetime.replace(
        tzinfo=utc)

    interloc_results = []
    thresholds = []
    wfs = []

    starttime = event_time - timedelta(seconds=1.5)
    endtime = event_time + timedelta(seconds=1.5)

    # from pdb import set_trace; set_trace()
    complete_wf = web_client.get_continuous(base_url, starttime, endtime,
                                            sites, utc)

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

        station_codes = np.unique([tr.stats.station for tr in wf])

        recovery_fraction = len(station_codes) / len(sites)

        if recovery_fraction < minimum_recovery_fraction:
            logger.warning('Recovery fraction %0.2f is below recovery '
                           'fraction threshold of %0.2f set in the config '
                           'file' % (recovery_fraction,
                                     minimum_recovery_fraction))

            return False

        clean_data_processor = clean_data.Processor()
        clean_wf = clean_data_processor.process(stream=wf)

        interloc_processor = interloc.Processor()
        results = interloc_processor.process(stream=clean_wf)
        new_cat = interloc_processor.output_catalog(cat)
        results['catalog'] = new_cat
        interloc_results.append(results)
        wfs.append(wf)
        thresholds.append(results['normed_vmax'])

    # from pdb import set_trace; set_trace()

    # from pdb import set_trace;
    # set_trace()

    index = np.argmax(thresholds)

    logger.info('Event location: {}'.format(new_cat[0].preferred_origin().loc))
    logger.info('Event time: {}'.format(new_cat[0].preferred_origin().time))

    return interloc_results[index]


def get_waveforms(interloc_dict, event):

    utc_time = datetime.fromtimestamp(interloc_dict['event_time'])
    local_time = utc_time.replace(tzinfo=utc)
    starttime = local_time - timedelta(seconds=0.5)
    endtime = local_time + timedelta(seconds=1.5)

    fixed_length_wf = web_client.get_continuous(base_url, starttime, endtime,
                                                sites, utc)

    # finding the station that is the closest to the event
    starttime = local_time - timedelta(seconds=10)
    endtime = local_time + timedelta(seconds=10)
    dists = []
    stations = []
    ev_loc = np.array([interloc_dict['x'], interloc_dict['y'],
                       interloc_dict['z']])

    for station in inventory.stations():
        dists.append(np.linalg.norm(ev_loc - station.loc))
        stations.append(station.code)

    indices = np.argsort(dists)

    context_trace_filter = settings.get('data_connector').context_trace.filter

    for i in indices:
        if stations[i] in settings.get('sensors').black_list:
            continue
        logger.info('getting context trace for station {}'.format(stations[i]))
        context = web_client.get_continuous(base_url, starttime, endtime,
                                            [stations[i]], utc)
        context.filter('bandpass', **context_trace_filter)

        if context:
            break

    waveforms = {'stream': fixed_length_wf,
                 'context': context}

    return waveforms


def record_noise_event(cat):
    processed_events_db = settings.get('mongo_db').db_processed_events
    db = mongo_client[processed_events_db]
    collection = db['noise_event']

    document = {'event_id': cat[0].resource_id,
                'processed_time_stamp': datetime.now().timestamp()}

    collection.insert_one(document)


def send_to_api(cat, waveforms):
    api_base_url = settings.get('api_base_url')
    post_data_from_objects(api_base_url, event_id=None,
                           event=cat,
                           stream=waveforms['stream'],
                           context_stream=waveforms['context'],
                           variable_length_stream=waveforms[
                               'variable_length'],
                           tolerance=None,
                           send_to_bus=False)


def send_to_automatic_processing(cat, waveforms):

    automatic_message_queue = settings.get(
        'processing_flow').automatic.message_queue

    out_dict = {}

    wf_io = BytesIO()
    waveforms['stream'].write(wf_io, format='mseed')
    out_dict['waveform_bytes'] = wf_io.getvalue()

    context_io = BytesIO()
    waveforms['context'].write(context_io, format='mseed')
    out_dict['context_bytes'] = context_io.getvalue()

    event_io = BytesIO()
    cat.write(event_io, format='quakeml')
    out_dict['event_bytes'] = event_io.getvalue()

    msg = msgpack.dumps(out_dict)

    redis.lpush(automatic_message_queue, msg)


def resend_to_redis(cat, processing_attempts):
    file_out = BytesIO()
    cat.write(file_out, format='quakeml')

    connector_settings = settings.get('data_connector')
    maximum_attempts = connector_settings.maximum_attempts
    minimum_delay_minutes = connector_settings.minimum_delay_minutes

    event_time_ts = cat[0].preferred_origin().time.timestamp
    delay = datetime.now().timestamp - event_time_ts

    if delay > minimum_delay_minutes * 60:
        processing_attempts += 1

    if processing_attempts > maximum_attempts:
        logger.info('maximum number of attempts reached, this event will '
                    'never be processed!')

    dict_out = {'event_bytes': file_out.getvalue(),
                'processing_attempts': processing_attempts}

    msg = msgpack.dumps(dict_out)
    redis.rpush(message_queue, msg)


while 1:
    logger.info('waiting for message on channel %s' % message_queue)
    message_queue, message = redis.blpop(message_queue)
    logger.info('message received')

    tmp = msgpack.loads(message, raw=False)
    processing_attempts = tmp['processing_attempts']
    cat = read_events(BytesIO(tmp['event_bytes'].encode()))

    try:

        variable_length_wf = web_client.get_seismogram_event(base_url, cat[0],
                                                             network_code, utc)

        interloc_results = interloc_election(cat)

        if not interloc_results:
            continue
        new_cat = interloc_results['catalog']
        waveforms = get_waveforms(interloc_results, new_cat)
        waveforms['variable_length'] = variable_length_wf

        logger.info('event classification')
        category = event_classifier.Processor().process(stream=waveforms[
            'context'])

        logger.info('event categorized as {}'.format(category['microquake']))

        file_name = str(new_cat[0].preferred_origin().time) + '.mseed'
        waveforms['context'].write(file_name)

        from pdb import set_trace
        set_trace()

        if category['microquake'].lower() == 'noise':
            # record_noise_event(new_cat)
            logger.info('event categorized as noise are not further processed '
                        'and will not be saved in the database')

            continue

        new_cat[0].event_type = category['quakeml']

        logger.info('sending to API, will take long time')

        send_to_api(new_cat, waveforms)

        logger.info('sending to automatic pipeline')
        send_to_automatic_processing(cat, waveforms)

        logger.info('done collecting the waveform, happy processing!')

    except KeyboardInterrupt:
        break

    except:
        logger.info('processing failed, on attempt {}'.format(
            processing_attempts))
        resend_to_redis(cat, processing_attempts)
