import pytest
from tests.helpers.data_utils import get_test_data

from microquake.core.event import OriginUncertainty
from spp.pipeline.nlloc import Processor

test_data_name = "test_output_picker"


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


def test_hypocenter_location(catalog, waveform_stream):
    processor = Processor()
    processor.process(cat=catalog, stream=waveform_stream)
    output_catalog = processor.output_catalog(catalog)

    check_hypocenter_location((catalog, waveform_stream), output_catalog)


def check_hypocenter_location(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    assert input_catalog[0].preferred_origin().origin_uncertainty is None
    origin_uncertainty = output_catalog[0].preferred_origin().origin_uncertainty
    assert isinstance(origin_uncertainty, OriginUncertainty)
    assert origin_uncertainty.confidence_ellipsoid.semi_major_axis_length > 0

    assert origin_uncertainty.confidence_ellipsoid.semi_minor_axis_length > 0

    assert origin_uncertainty.confidence_ellipsoid.semi_intermediate_axis_length > 0

    origin = output_catalog[0].preferred_origin()

    for arr in origin.arrivals:
        assert arr.hypo_dist_in_m == arr.distance
