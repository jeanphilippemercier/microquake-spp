import numpy as np
import pytest

from microquake.core import read_events
from microquake.core.event import Origin
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication


def test_interloc():
    with open('./data/tests/test_input_interloc.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('./data/tests/test_input_interloc.mseed', "rb") as event_file:
        waveform_stream = read(event_file, format="MSEED")

    original_origin_count = len(catalog[0].origins)

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

    cat, stream = cli.app.output_data

    assert len(cat[0].origins) == (original_origin_count + 1)
    assert isinstance(cat[0].origins[0], Origin)
    assert cat[0].preferred_origin_id is not None

    dist = np.linalg.norm(cat[0].origins[0].loc - cat[0].origins[1].loc)
    assert 170 < dist < 180

    assert cat[0].preferred_origin().extra.interloc_normed_vmax.value > 0
    assert cat[0].preferred_origin().extra.interloc_normed_vmax.namespace == 'MICROQUAKE'
    
    assert len(stream) == len(waveform_stream)
