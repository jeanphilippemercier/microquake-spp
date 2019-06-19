import pytest
from tests.helpers.data_utils import get_test_data

from spp.pipeline.magnitude import Processor

test_data_name = "test_output_energy"


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


def test_magnitude(catalog, waveform_stream):
    processor = Processor()
    res = processor.process(cat=catalog.copy())

    check_magnitude_data((catalog, waveform_stream), res['cat'])


def check_magnitude_data(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    input_magnitude_count = len(input_catalog[0].magnitudes)

    event = output_catalog[0]
    assert event.magnitudes
    assert len(event.magnitudes) > input_magnitude_count
    assert event.station_magnitudes
