import pytest
from tests.helpers.data_utils import get_test_data

from microquake.processors.picker import Processor

test_data_name = "test_output_interloc"


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


def test_picker(catalog, waveform_stream):
    processor = Processor()
    processor.process(cat=catalog, stream=waveform_stream)
    output_catalog = processor.output_catalog(catalog)

    check_picker_data((catalog, waveform_stream), output_catalog)


def check_picker_data(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    original_pick_count = len(input_catalog[0].picks)
    assert len(output_catalog[0].picks) > original_pick_count
