import pytest

from microquake.core import read_events
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication


def test_interloc():
    with open('./data/tests/test_input_interloc.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('./data/tests/test_input_interloc.mseed', "rb") as event_file:
        waveform_stream = read(event_file, format="MSEED")

    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='interloc', processing_flow_name="automatic", input_data=test_input)

    args = AttribDict({
        'mode': 'local',
        'module':'interloc',
        'settings_name':'interloc',
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
    cli = CLI('interloc', 'automatic', app=test_app, args=args)

    cli.prepare_module()
    # assert cli.prepared_objects['test_prep_dep']

    cli.run_module()
    # assert cli.app.process_output['test_output']
    # self.output_data = (cat, stream)
