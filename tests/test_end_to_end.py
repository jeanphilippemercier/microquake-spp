import pytest

from microquake.core import read_events
from microquake.core.stream import read
from microquake.core.util.attribdict import AttribDict
from spp.utils.cli import CLI
from spp.utils.test_application import TestApplication
from tests.test_focal_mechanism import check_focal_mechanism_data
from tests.test_interloc import check_interloc_data_end_to_end
from tests.test_magnitude import check_magnitude_data
from tests.test_magnitude_f import check_magnitude_f_data
from tests.test_measure_amplitudes import check_amplitudes_data
from tests.test_measure_energy import check_measure_energy_data
from tests.test_measure_smom import check_smom_data
from tests.test_nlloc import check_hypocenter_location
from tests.test_picker import check_picker_data


def test_end_to_end():
    with open('./data/tests/test_end_to_end.xml', "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open('./data/tests/test_end_to_end.mseed', "rb") as event_file:
        waveform_stream = read(event_file, format="MSEED")

    test_input = (catalog, waveform_stream)
    test_app = TestApplication(module_name='interloc', processing_flow_name="automatic", input_data=test_input)

    args = AttribDict({
        'mode': 'local',
        'module': None,
        'modules':'interloc,picker,nlloc,measure_amplitudes,measure_smom,focal_mechanism,measure_energy,magnitude,magnitude_f',
        'settings_name':None,
        'processing_flow':'automatic',
        'input_bytes':None,
        'input_mseed':None,
        'input_quakeml':None,
        'output_bytes':None,
        'output_mseed':None,
        'output_quakeml':None,
        'event_id':None,
        'send_to_api':None,
    })
    cli = CLI('chain', 'automatic', app=test_app, args=args)

    cli.prepare_module()

    cli.run_module()

    # We seem to lose the preferred_origin() reference, so run this test separately
    output_data = test_app.clean_message(cli.app.output_data)
    input_data = test_app.clean_message((catalog, waveform_stream))
    check_hypocenter_location(input_data, output_data)

    input_data = test_app.clean_message((catalog, waveform_stream))
    check_end_to_end_data(input_data, cli.app.output_data)


def check_end_to_end_data(input_data, output_data):
    check_interloc_data_end_to_end(input_data, output_data)
    check_picker_data(input_data, output_data)
    check_amplitudes_data(input_data, output_data)
    check_smom_data(input_data, output_data)
    check_focal_mechanism_data(input_data, output_data)
    check_measure_energy_data(input_data, output_data)
    check_magnitude_data(input_data, output_data)
    check_magnitude_f_data(input_data, output_data)
