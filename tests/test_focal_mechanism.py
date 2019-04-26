import pytest

from microquake.core import read_events
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication


def test_focal_mechanism():
    with open('/app/data/tests/test_output_smom.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('/app/data/tests/test_output_smom.mseed', "rb") as event_file:
        waveform_stream = read(event_file, format="MSEED")

    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='focal_mechanism', processing_flow_name="automatic", input_data=test_input)

    args = AttribDict({
        'mode': 'local',
        'module':'focal_mechanism',
        'settings_name':'focal_mechanism',
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
    cli = CLI('focal_mechanism', 'automatic', app=test_app, args=args)

    cli.prepare_module()
    cli.run_module()
    check_focal_mechanism_data((catalog, waveform_stream), cli.app.output_data)


def check_focal_mechanism_data(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data

    for event in output_catalog:
        assert len(event.focal_mechanisms) > 0
        assert event.preferred_focal_mechanism_id
        for focal_mechanism in event.focal_mechanisms:
            assert focal_mechanism.station_polarity_count > 0

    assert len(output_waveform_stream) == len(input_waveform_stream)
