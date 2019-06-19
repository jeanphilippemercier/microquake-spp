import pytest
from tests.helpers.data_utils import get_test_data

from spp.pipeline.focal_mechanism import Processor

test_data_name = "test_output_smom"


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


def test_focal_mechanism(catalog, waveform_stream):
    processor = Processor()
    res = processor.process(cat=catalog, stream=waveform_stream)

    check_focal_mechanism_data((catalog, waveform_stream), res['cat'])


def check_focal_mechanism_data(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    for event in output_catalog:
        assert len(event.focal_mechanisms) > 0
        assert event.preferred_focal_mechanism_id

        for focal_mechanism in event.focal_mechanisms:
            assert focal_mechanism.station_polarity_count > 0
