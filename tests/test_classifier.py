import pytest
from microquake.ml.classifier import SeismicModel
import numpy as np
from obspy.core.stream import Stream
import microquake.core as mqk
pytest.test_data_name = "test_classifier"


def test_classifier():
    sc = SeismicModel()
    assert not sc is None
