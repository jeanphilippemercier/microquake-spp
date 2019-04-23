import pytest

from microquake.core import read_events
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication


def test_measure_energy():
    with open('./data/tests/test_measure_energy.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('./data/tests/test_measure_energy.mseed', "rb") as event_file:
        waveform_stream = read(event_file, format="MSEED")

    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='measure_energy', processing_flow_name="automatic", input_data=test_input)

    args = AttribDict({
        'mode': 'local',
        'module':'measure_energy',
        'settings_name':'measure_energy',
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
    cli = CLI('measure_energy', 'automatic', app=test_app, args=args)

    cli.prepare_module()
    cli.run_module()
    check_measure_energy_data((catalog, waveform_stream), cli.app.output_data)


def check_measure_energy_data(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data

    for event in output_catalog:
        origin = event.preferred_origin() if event.preferred_origin() else event.origins[0]
        for arr in origin.arrivals:
            assert abs(arr.vel_flux) > 0
            assert abs(arr.energy) > 0

    assert len(output_waveform_stream) == len(input_waveform_stream)
