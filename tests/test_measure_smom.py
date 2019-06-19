import pytest
from tests.helpers.data_utils import get_test_data

from spp.pipeline.measure_smom import Processor

test_data_name = "test_output_amplitude"


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


def test_measure_smom(catalog, waveform_stream):
    processor = Processor()
    processor.process(cat=catalog, stream=waveform_stream)
    output_catalog = processor.output_catalog(catalog)

    check_smom_data((catalog, waveform_stream), output_catalog)


def check_smom_data(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    for event in output_catalog:
        for arr in event.preferred_origin().arrivals:
            if arr.smom:
                assert abs(arr.smom) > 0
                assert abs(arr.fit) > 0
                assert abs(arr.tstar) > 0
