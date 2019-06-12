import pytest
from tests.helpers.data_utils import get_test_data

from spp.pipeline.measure_energy import Processor

test_data_name = "test_output_focal_mechanism"


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


def test_measure_energy(catalog, waveform_stream):
    processor = Processor(module_name="measure_energy")
    res = processor.process(cat=catalog, stream=waveform_stream)

    check_measure_energy_data((catalog, waveform_stream), res['cat'])


def check_measure_energy_data(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    for event in output_catalog:
        origin = event.preferred_origin() if event.preferred_origin() else event.origins[0]

        for arr in origin.arrivals:
            assert abs(arr.vel_flux) > 0
            assert abs(arr.energy) > 0
