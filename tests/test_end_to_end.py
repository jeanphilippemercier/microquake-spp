import pytest
from tests.helpers.data_utils import clean_test_data, get_test_data
from tests.test_focal_mechanism import check_focal_mechanism_data
from tests.test_interloc import check_interloc_data_end_to_end
from tests.test_magnitude import check_magnitude_data
from tests.test_magnitude_f import check_magnitude_f_data
from tests.test_measure_amplitudes import check_amplitudes_data
from tests.test_measure_energy import check_measure_energy_data
from tests.test_measure_smom import check_smom_data
from tests.test_nlloc import check_hypocenter_location
from tests.test_picker import check_picker_data

from microquake.core import read_events
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.pipeline import (AnalyseSignalProcessor, EventDatabaseProcessor,
                          FocalMechanismProcessor, InterlocProcessor,
                          MagnitudeProcessor, MeasureAmplitudesProcessor,
                          MeasureEnergyProcessor, MeasureSmomProcessor,
                          NLLocProcessor, PickerProcessor, RayTracerProcessor)
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication

test_data_name = "test_end_to_end"

@pytest.fixture
def catalog():
    file_name = test_data_name + ".xml"
    test_data = get_test_data(file_name, "QUAKEML")
    yield test_data


@pytest.fixture
def waveform_stream():
    file_name = test_data_name + ".mseed"
    test_data = get_test_data(file_name, "MSEED")
    yield test_data
    clean_test_data(file_name)


def test_end_to_end(catalog, waveform_stream):
    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='interloc', processing_flow_name="automatic", input_data=test_input)

    processor = InterlocProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)

    processor = PickerProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)

    processor = NLLocProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    processor = MeasureAmplitudesProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    processor = MeasureSmomProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    processor = FocalMechanismProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    processor = MeasureEnergyProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    processor = MagnitudeProcessor()
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    processor = MagnitudeProcessor(module_type='frequency')
    processor.process(stream=waveform_stream, cat=catalog)
    catalog = processor.output_catalog(catalog)
    # We seem to lose the preferred_origin() reference, so run this test separately
    input_data = test_app.clean_message((catalog, waveform_stream))
    check_hypocenter_location(input_data, output_catalog)

    input_data = test_app.clean_message((catalog, waveform_stream))
    check_end_to_end_data(input_data, output_catalog)


def check_end_to_end_data(input_data, output_catalog):
    check_interloc_data_end_to_end(input_data, output_data)
    check_picker_data(input_data, output_data)
    check_amplitudes_data(input_data, output_data)
    check_smom_data(input_data, output_data)
    check_focal_mechanism_data(input_data, output_data)
    check_measure_energy_data(input_data, output_data)
    check_magnitude_data(input_data, output_data)
    check_magnitude_f_data(input_data, output_data)
