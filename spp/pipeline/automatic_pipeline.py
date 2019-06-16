from datetime import datetime
from spp.utils.application import Application
from spp.pipeline import (interloc, picker, nlloc, measure_amplitudes,
                          measure_smom, focal_mechanism, measure_energy,
                          magnitude, event_database)
# from spp.core.serializers.serializer import Seismic
from loguru import logger
from microquake.core import read, read_events, UTCDateTime
from microquake.core.event import Origin, AttribDict
from spp.core.settings import settings
import numpy as np
from io import BytesIO
import asyncio


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

    automatic_pipeline(cat, fixed_length_wf)


def picker_election(cat, fixed_length):
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

    picker_hf_processor.process(stream=fixed_length, cat=cat)
    picker_mf_processor.process(stream=fixed_length, cat=cat)
    picker_lf_processor.process(stream=fixed_length, cat=cat)

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



    #
    # residuals = [catalog[0].preferred_origin().residual
    #              for catalog in cat_pickers]

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

    eventdb_processor = event_database.Processor()

    interloc_processor = interloc.Processor()
    interloc_processor.process(stream=fixed_length)
    cat_interloc = interloc_processor.output_catalog(cat)

    result = eventdb_processor.process(cat=cat_interloc)

    # send to database

    cat_picker = picker_election(cat_interloc, fixed_length)
    nlloc_processor = nlloc.Processor()
    nlloc_processor.initializer()
    cat_nlloc = nlloc_processor.process(cat=cat_picker)['cat']

    # Removing the Origin object used to hold the picks
    del cat_nll[0].Origins[-2]

    result = eventdb_processor.process(cat=cat_nlloc)

    # send to data base

    measure_amplitudes_processor = measure_amplitudes.Processor()
    cat_amplitude = measure_amplitudes_processor.process(cat=cat_nlloc,
                                             stream=fixed_length)['cat']

    smom_processor = measure_smom.Processor()
    cat_smom = smom_processor.process(cat=cat_amplitude,
                                      stream=fixed_length)['cat']

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
