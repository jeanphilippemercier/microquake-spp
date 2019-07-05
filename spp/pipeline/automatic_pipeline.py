from spp.pipeline import (event_classifier, interloc, picker, nlloc,
                          measure_amplitudes, measure_smom, focal_mechanism,
                          measure_energy, magnitude, event_database,
                          clean_data)

from spp.utils.seismic_client import put_event_from_objects

from loguru import logger
from microquake.core import read, read_events, UTCDateTime
from microquake.core.event import (Catalog, Event)
from microquake.core.stream import (Stream)
from spp.core.settings import settings
import numpy as np
from io import BytesIO
from redis import Redis
import msgpack


redis = Redis(**settings.get('redis_db'))
ray_tracer_message_queue = settings.get(
    'processing_flow').ray_tracing.message_queue
automatic_message_queue = settings.get(
    'processing_flow').automatic.message_queue
api_base_url = settings.get('api_base_url')


def picker_election(location, event_time_utc, cat, stream):
    """
    Calculates the picks using 1 method but different
    parameters and then retains the best set of picks. The function is
    current calling the picker sequentially processes should be spanned so
    the three different picker are running over three distinct threads.
    :param cat: Catalog
    :param fixed_length: fixed length seismogram
    :return: a Catalog containing the response of the picker that performed
    best according to some logic described in this function.
    """
    picker_hf_processor = picker.Processor(module_type='high_frequencies')
    picker_mf_processor = picker.Processor(module_type='medium_frequencies')
    picker_lf_processor = picker.Processor(module_type='low_frequencies')

    picker_hf_processor.process(stream=stream, location=location,
                                event_time_utc=event_time_utc)
    picker_mf_processor.process(stream=stream, location=location,
                                event_time_utc=event_time_utc)
    picker_lf_processor.process(stream=stream, location=location,
                                event_time_utc=event_time_utc)

    cat_picker_hf = picker_hf_processor.output_catalog(cat.copy())
    cat_picker_mf = picker_mf_processor.output_catalog(cat.copy())
    cat_picker_lf = picker_lf_processor.output_catalog(cat.copy())

    cat_pickers = [cat_picker_hf, cat_picker_mf, cat_picker_lf]

    len_arrivals = [len(catalog[0].preferred_origin().arrivals)
                    for catalog in cat_pickers]

    logger.info('Number of arrivals for each picker:\n'
                'High Frequencies picker   : %d \n'
                'Medium Frequencies picker : %d \n'
                'Low Frequencies picker    : %d \n' % (len_arrivals[0],
                                                       len_arrivals[1],
                                                       len_arrivals[2]))

    imax = np.argmax(len_arrivals)

    return cat_pickers[imax]


def automatic_pipeline(waveform_bytes=None, context_bytes=None,
                       event_bytes=None):
    """
    automatic pipeline
    :param stream_bytes: fixed length stream encoded as mseed
    :param context_bytes: context trace encoded as mseed
    :param cat_bytes: catalog object encoded in quakeml
    :return:
    """

    stream = read(BytesIO(waveform_bytes), format='mseed')
    context = read(BytesIO(context_bytes), format='mseed')

    if event_bytes is None:
        logger.info('No catalog was provided creating new')
        cat = Catalog(events=[Event()])
    else:
        cat = read_events(BytesIO(event_bytes), format='quakeml')

    from pdb import set_trace; set_trace()
    event_id = cat[0].resource_id

    logger.info('removing traces for sensors in the black list, or are '
                'filled with zero, or contain NaN')
    clean_data_processor = clean_data.Processor()
    stream = clean_data_processor.process(stream=stream)

    loc = cat[0].preferred_origin().loc
    event_time_utc = cat[0].preferred_origin().time

    cat_picker = picker_election(loc, event_time_utc, cat, stream)
    nlloc_processor = nlloc.Processor()
    nlloc_processor.initializer()
    cat_nlloc = nlloc_processor.process(cat=cat_picker)['cat']

    # Removing the Origin object used to hold the picks
    del cat_nlloc[0].origins[-2]

    loc = cat_nlloc[0].preferred_origin().loc
    event_time_utc = cat_nlloc[0].preferred_origin().time
    picker_sp_processor = picker.Processor(module_type='second_pass')
    picker_sp_processor.process(stream=stream, location=loc,
                                event_time_utc=event_time_utc)

    cat_picker = picker_sp_processor.output_catalog(cat_nlloc)

    nlloc_processor = nlloc.Processor()
    nlloc_processor.initializer()
    cat_nlloc = nlloc_processor.process(cat=cat_picker)['cat']

    # Removing the Origin object used to hold the picks
    del cat_nlloc[0].origins[-2]

    bytes_out = BytesIO()
    cat_nlloc.write(bytes_out, format='QUAKEML')

    logger.info('sending request to the ray tracer on channel %s'
                % ray_tracer_message_queue)

    data_out = {'event_bytes': bytes_out.getvalue()}
    msg = msgpack.dumps(data_out)

    redis.rpush(ray_tracer_message_queue, msg)

    # send to data base
    cat_nlloc[0].resource_id = event_id
    put_event_from_objects(api_base_url, event_id, event=cat_nlloc,
                           waveform=stream)

    measure_amplitudes_processor = measure_amplitudes.Processor()
    cat_amplitude = measure_amplitudes_processor.process(cat=cat_nlloc,
                                             stream=stream)['cat']



    smom_processor = measure_smom.Processor()
    cat_smom = smom_processor.process(cat=cat_amplitude,
                                      stream=stream)['cat']

    fmec_processor = focal_mechanism.Processor()
    cat_fmec = fmec_processor.process(cat=cat_smom,
                                      stream=stream)['cat']

    energy_processor = measure_energy.Processor()
    cat_energy = energy_processor.process(cat=cat_fmec,
                                          stream=stream)['cat']

    magnitude_processor = magnitude.Processor()
    cat_magnitude = magnitude_processor.process(cat=cat_energy,
                                                stream=stream)['cat']

    magnitude_f_processor = magnitude.Processor(module_type = 'frequency')
    cat_magnitude_f = magnitude_f_processor.process(cat=cat_magnitude,
                                                    stream=stream)['cat']


    cat_magnitude_f[0].resource_id = event_id
    put_event_from_objects(api_base_url, event_id, event=cat_magnitude_f)

    return cat_magnitude_f
