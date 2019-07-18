import pytest
from tests.helpers.data_utils import get_test_data
from tests.test_focal_mechanism import check_focal_mechanism_data
from tests.test_interloc import check_interloc_data_end_to_end
from tests.test_magnitude import check_magnitude_data
from tests.test_magnitude_f import check_magnitude_f_data
from tests.test_measure_amplitudes import check_amplitudes_data
from tests.test_measure_energy import check_measure_energy_data
from tests.test_measure_smom import check_smom_data
from tests.test_nlloc import check_hypocenter_location
from tests.test_picker import check_picker_data

from spp.pipeline.focal_mechanism import Processor as FocalMechanismProcessor
from spp.pipeline.interloc import Processor as InterlocProcessor
from spp.pipeline.magnitude import Processor as MagnitudeProcessor
from spp.pipeline.measure_amplitudes import Processor as MeasureAmplitudesProcessor
from spp.pipeline.measure_energy import Processor as MeasureEnergyProcessor
from spp.pipeline.measure_smom import Processor as MeasureSmomProcessor
from spp.pipeline.nlloc import Processor as NLLocProcessor
from spp.pipeline.picker import Processor as PickerProcessor

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


def test_end_to_end(catalog, waveform_stream):
    input_data = (catalog, waveform_stream)
    # test_app = TestApplication(module_name='interloc', processing_flow_name="automatic", input_data=test_input)

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
    # input_data = test_app.clean_message((catalog, waveform_stream))
    check_hypocenter_location(input_data, catalog)

    # input_data = test_app.clean_message((catalog, waveform_stream))
    check_end_to_end_data(input_data, catalog)


def check_end_to_end_data(input_data, output_catalog):
    check_interloc_data_end_to_end(input_data, output_catalog)
    check_picker_data(input_data, output_catalog)
    check_amplitudes_data(input_data, output_catalog)
    check_smom_data(input_data, output_catalog)
    check_focal_mechanism_data(input_data, output_catalog)
    check_measure_energy_data(input_data, output_catalog)
    check_magnitude_data(input_data, output_catalog)
    check_magnitude_f_data(input_data, output_catalog)
