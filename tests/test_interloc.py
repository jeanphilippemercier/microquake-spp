import numpy as np
import pytest

from microquake.core import read_events
from microquake.core.event import Origin
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication


def test_interloc():
    with open('/app/data/tests/test_end_to_end.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('/app/data/tests/test_end_to_end.mseed', "rb") as event_file:
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
    cli.run_module()

    check_interloc_data((catalog, waveform_stream), cli.app.output_data)


def check_interloc_data(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data

    original_origin_count = len(input_catalog[0].origins)

    assert len(output_catalog[0].origins) == (original_origin_count + 1)
    assert isinstance(output_catalog[0].origins[0], Origin)
    assert output_catalog[0].preferred_origin_id is not None

    dist = np.linalg.norm(output_catalog[0].origins[0].loc - output_catalog[0].origins[1].loc)
    assert 21 < dist < 22

    assert output_catalog[0].preferred_origin().extra.interloc_normed_vmax.value is not None
    assert output_catalog[0].preferred_origin().extra.interloc_normed_vmax.namespace == 'MICROQUAKE'
    
    assert len(output_waveform_stream) == len(input_waveform_stream)

# Some interloc data is removed later in the pipeline
def check_interloc_data_end_to_end(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data
    assert isinstance(output_catalog[0].origins[0], Origin)
    assert output_catalog[0].preferred_origin_id is not None

    dist = np.linalg.norm(output_catalog[0].origins[0].loc - output_catalog[0].origins[1].loc)
    assert 21 < dist < 22
    assert len(output_waveform_stream) == len(input_waveform_stream)
