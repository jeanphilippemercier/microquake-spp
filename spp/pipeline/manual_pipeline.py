from spp.pipeline import (nlloc, measure_amplitudes,
                          measure_smom, focal_mechanism, measure_energy,
                          magnitude, event_database)
from loguru import logger
from spp.core.settings import settings
from io import BytesIO
from redis import Redis
from microquake.core import read, read_events

redis = Redis(**settings.get('redis_db'))
ray_tracer_message_queue = settings.get(
    'processing_flow').ray_tracing.message_queue
manual_message_queue = settings.get(
    'processing_flow').manual.message_queue


def manual_pipeline(waveform_bytes=None, event_bytes=None):
    """
    manual or interactive pipeline
    :param stream_bytes:
    :param cat_bytes:
    :return:
    """

    stream = read(BytesIO(waveform_bytes), format='mseed')
    cat = read_events(BytesIO(event_bytes), format='quakeml')

    eventdb_processor = event_database.Processor()
    eventdb_processor.initializer()

    # Error in postion data to the API. Returned with error code 400: bad
    # request

    nlloc_processor = nlloc.Processor()
    nlloc_processor.initializer()
    cat_nlloc = nlloc_processor.process(cat=cat)['cat']

    # Send the NLLOC result to the database
    result = eventdb_processor.process(cat=cat_nlloc)

    # Removing the Origin object used to hold the picks
    del cat_nlloc[0].origins[-2]


    # calculating the rays asynchronously
    bytes_out = BytesIO()
    cat_nlloc.write(bytes_out, format='QUAKEML')

    logger.info('sending request to the ray tracer on channel %s'
                % ray_tracer_message_queue)
    redis.rpush(ray_tracer_message_queue, bytes_out.getvalue())


    measure_amplitudes_processor = measure_amplitudes.Processor()
    cat_amplitude = measure_amplitudes_processor.process(cat=cat_nlloc,
                                             stream=stream)['cat']



    smom_processor = measure_smom.Processor()
    cat_smom = smom_processor.process(cat=cat_amplitude,
                                      stream=stream)['cat']

    # TESTED UP TO THIS POINT, THE CONTAINER DOES NOT CONTAIN THE MOST
    # RECENT VERSION OF THE HASHWRAPPER LIBRARY AND CANNOT RUN
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

    result = eventdb_processor.process(cat=cat_magnitude_f)

    return cat_magnitude_f

if __name__ == '__main__':
    test_automatic_pipeline()
