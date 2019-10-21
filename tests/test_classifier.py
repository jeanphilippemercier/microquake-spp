import pytest
from microquake.ml.classifier import SeismicModel

pytest.test_data_name = "test_classifier"


def test_classifier():
    sc = SeismicModel()
    assert sc is not None
