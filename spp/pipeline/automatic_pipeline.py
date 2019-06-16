from datetime import datetime
from spp.utils.application import Application
from spp.pipeline import (interloc, picker, nlloc, measure_amplitudes,
                          measure_smom, focal_mechanism, measure_energy,
                          magnitude, event_database, ray_tracer)
# from spp.core.serializers.serializer import Seismic
from loguru import logger
from microquake.core import read, read_events, UTCDateTime
from microquake.core.event import (Catalog, Event, Origin, AttribDict)
from spp.core.settings import settings
import numpy as np
from io import BytesIO
import asyncio
from redis import StrictRedis
import pickle

redis = StrictRedis(**settings.get('redis_db'))
ray_tracer_message_queue = settings.get(
    'processing_flow').ray_tracing.message_queue
automatic_message_queue = settings.get(
    'processing_flow').automatic.message_queue


def test_automatic_pipeline():
    logger.info('loading mseed data')
    # mseed_bytes = requests.get("https://permanentdbfilesstorage.blob.core"
    #                            ".windows.net/permanentdbfilesblob/events/2019-06"
    #                            "-09T033053.080047Z.mseed").content
    #
    # with open('test_data.mseed', 'wb') as fout:
    #     fout.write(mseed_bytes)

    with open('test_data.mseed', 'rb') as fin:
        mseed_bytes = fin.read()

    logger.info('done loading mseed data')

    fixed_length_wf = read(BytesIO(mseed_bytes), format='mseed')

    logger.info('loading catalogue data')
    # catalog_bytes = requests.get(
    #     "https://permanentdbfilesstorage.blob.core.windows.net"
    #     "/permanentdbfilesblob/events/2019-06-09T033053.047217Z.xml").content
    #
    # with open('test_data.xml', 'wb') as fout:
    #     fout.write(catalog_bytes)

    with open('test_data.xml', 'rb') as fin:
        catalog_bytes = fin.read()

    logger.info('done loading catalogue data')

    cat = read_events(BytesIO(catalog_bytes), format='quakeml')

    # bytes_out = BytesIO()
    # fixed_length_wf.write(bytes_out, format='mseed')
    #
    # logger.info('sending request to the ray tracer on channel %s'
    #             % ray_tracer_message_queue)
    # redis.rpush(automatic_message_queue, bytes_out.getvalue())

    automatic_pipeline(fixed_length_wf)




def picker_election(location, event_time_utc, cat, fixed_length):
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

    picker_hf_processor.process(stream=fixed_length, location=location,
                                event_time_utc=event_time_utc)
    picker_mf_processor.process(stream=fixed_length, location=location,
                                event_time_utc=event_time_utc)
    picker_lf_processor.process(stream=fixed_length, location=location,
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


def automatic_pipeline(fixed_length, cat=None, context=None,
                       variable_length=None):
    """
    The pipeline for the automatic processing of the seismic data
    :param fixed_length: fixed length seismogram
    (microsquake.core.stream.Stream)
    :param cat: catalog (microquake.core.event.Catalog)
    :param context: context trace (microquake.core.stream.Stream)
    :param variable_length: variable length seismogram (
    microquake.core.stream.Stream)
    :return: None
    """

    if not cat:
        cat = Catalog(events=[Event()])

    eventdb_processor = event_database.Processor()

    interloc_processor = interloc.Processor()
    interloc_results = interloc_processor.process(stream=fixed_length)
    loc = [interloc_results['x'],
           interloc_results['y'],
           interloc_results['z']]
    event_time_utc = UTCDateTime(interloc_results['event_time'])
    cat_interloc = interloc_processor.output_catalog(cat)

    eventdb_processor.initializer()

    # Error in postion data to the API. Returned with error code 400: bad
    # request
    result = eventdb_processor.process(cat=cat_interloc, stream=fixed_length)

    # send to database

    cat_picker = picker_election(loc, event_time_utc, cat_interloc,
                                 fixed_length)
    nlloc_processor = nlloc.Processor()
    nlloc_processor.initializer()
    cat_nlloc = nlloc_processor.process(cat=cat_picker)['cat']

    # Removing the Origin object used to hold the picks
    del cat_nlloc[0].origins[-2]

    bytes_out = BytesIO()
    cat_nlloc.write(bytes_out, format='QUAKEML')

    logger.info('sending request to the ray tracer on channel %s'
                % ray_tracer_message_queue)
    redis.rpush(ray_tracer_message_queue, bytes_out.getvalue())

    # send to data base
    result = eventdb_processor.process(cat=cat_nlloc)


    measure_amplitudes_processor = measure_amplitudes.Processor()
    cat_amplitude = measure_amplitudes_processor.process(cat=cat_nlloc,
                                             stream=fixed_length)['cat']



    smom_processor = measure_smom.Processor()
    cat_smom = smom_processor.process(cat=cat_amplitude,
                                      stream=fixed_length)['cat']

    # TESTED UP TO THIS POINT, THE CONTAINER DOES NOT CONTAIN THE MOST
    # RECENT VERSION OF THE HASHWRAPPER LIBRARY AND CANNOT RUN
    fmec_processor = focal_mechanism.Processor()
    cat_fmec = fmec_processor.process(cat=cat_smom,
                                      stream=fixed_length)['cat']

    energy_processor = measure_energy.Processor()
    cat_energy = energy_processor.process(cat=cat_fmec,
                                          stream=fixed_length)['cat']

    magnitude_processor = magnitude.Processor()
    cat_magnitude = magnitude_processor.process(cat=cat_energy,
                                                stream=fixed_length)['cat']

    magnitude_f_processor = magnitude.Processor(module_type = 'frequency')
    cat_magnitude_f = magnitude_f_processor.process(cat=cat_magnitude,
                                                    stream=fixed_length)['cat']

    result = eventdb_processor.process(cat=cat_magnitude_f)

    return cat_magnitude_f

if __name__ == '__main__':
    test_automatic_pipeline()
