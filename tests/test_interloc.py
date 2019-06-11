from datetime import datetime

import pytest
from tests.helpers.data_utils import get_test_data

import numpy as np
from microquake.core.event import Origin
from spp.pipeline.interloc import Processor

test_data_name = "test_end_to_end"


def test_interloc(catalog, waveform_stream):
    processor = Processor(module_name="interloc")
    processor.process(stream=waveform_stream)
    output_catalog = processor.output_catalog(catalog)

    check_interloc_data((catalog, waveform_stream), output_catalog)


def check_interloc_data(input_data, output_catalog):
    (input_catalog, input_waveform_stream) = input_data

    original_origin_count = len(input_catalog[0].origins)

    assert len(output_catalog[0].origins) == (original_origin_count + 1)
    assert isinstance(output_catalog[0].origins[0], Origin)
    assert output_catalog[0].preferred_origin_id is not None

    dist = np.linalg.norm(output_catalog[0].origins[0].loc - output_catalog[0].origins[1].loc)
    assert 21 < dist < 22

    assert output_catalog[0].preferred_origin().extra.interloc_normed_vmax.value is not None
    assert output_catalog[0].preferred_origin().extra.interloc_normed_vmax.namespace == 'MICROQUAKE'

# Some interloc data is removed later in the pipeline


def check_interloc_data_end_to_end(input_data, output_data):
    (input_catalog, input_waveform_stream) = input_data
    (output_catalog, output_waveform_stream) = output_data
    assert isinstance(output_catalog[0].origins[0], Origin)
    assert output_catalog[0].preferred_origin_id is not None

    dist = np.linalg.norm(output_catalog[0].origins[0].loc - output_catalog[0].origins[1].loc)
    assert 21 < dist < 22
    assert len(output_waveform_stream) == len(input_waveform_stream)
