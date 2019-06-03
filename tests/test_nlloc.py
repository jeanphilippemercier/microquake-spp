import numpy as np
import pytest

from microquake.core import read_events
from microquake.core.event import Origin, OriginUncertainty
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication
from tests.helpers.data_utils import clean_test_data, get_test_data

test_data_name = "test_output_picker"

@pytest.fixture
def catalog():
    file_name = test_data_name + ".xml"
    test_data = get_test_data(file_name, "QUAKEML")
    yield test_data
    clean_test_data(file_name)


@pytest.fixture
def waveform_stream():
    file_name = test_data_name + ".mseed"
    test_data = get_test_data(file_name, "MSEED")
    yield test_data
    clean_test_data(file_name)


def test_hypocenter_location(catalog, waveform_stream):
    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='nlloc', processing_flow_name="automatic", input_data=test_input)

    args = AttribDict({
        'mode': 'local',
        'module':'nlloc',
        'settings_name':'nlloc',
        'processing_flow':'automatic',
        'modules':None,
        'input_bytes':None,
        'input_mseed':None,
        'input_quakeml':None,
        'output_bytes':None,
        'output_mseed':None,
        'output_quakeml':None,
        'event_id':None,
        'send_to_api':None,
    })
    cli = CLI('nlloc', 'automatic', app=test_app, args=args)

    cli.run_module()

    check_hypocenter_location((catalog, waveform_stream), cli.app.output_data)


def check_hypocenter_location(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data

    assert input_catalog[0].preferred_origin().origin_uncertainty is None
    origin_uncertainty = output_catalog[0].preferred_origin().origin_uncertainty
    assert isinstance(origin_uncertainty, OriginUncertainty)
    assert origin_uncertainty.confidence_ellipsoid.semi_major_axis_length > 0

    assert origin_uncertainty.confidence_ellipsoid.semi_minor_axis_length > 0

    assert origin_uncertainty.confidence_ellipsoid.semi_intermediate_axis_length > 0

    origin = output_catalog[0].preferred_origin()
    for arr in origin.arrivals:
        assert arr.hypo_dist_in_m == arr.distance

    assert len(output_waveform_stream) == len(input_waveform_stream)
