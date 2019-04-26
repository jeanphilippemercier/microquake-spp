import pytest

from microquake.core import read_events
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication


def test_picker():
    with open('/app/data/tests/test_end_to_end.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('/app/data/tests/test_end_to_end.mseed', "rb") as event_file:
        waveform_stream = read(event_file, format="MSEED")

    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='picker', processing_flow_name="automatic", input_data=test_input)

    args = AttribDict({
        'mode': 'local',
        'module':'picker',
        'settings_name':'picker',
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
    cli = CLI('picker', 'automatic', app=test_app, args=args)

    cli.prepare_module()
    cli.run_module()
    check_picker_data((catalog, waveform_stream), cli.app.output_data)


def check_picker_data(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data

    original_pick_count = len(input_catalog[0].picks)
    assert len(output_catalog[0].picks) > original_pick_count
    assert len(output_waveform_stream) == len(input_waveform_stream)
