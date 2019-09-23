import os

import microquake.core.settings as sppsettings
import pytest
from fakeredis import FakeStrictRedis

from .helpers.data_utils import get_test_data

pytest.test_data_name = None


@pytest.fixture(scope="function")
def catalog():
    file_name = pytest.test_data_name + ".xml"

    return get_test_data(file_name, "QUAKEML")


@pytest.fixture(scope="function")
def waveform_stream():
    file_name = pytest.test_data_name + ".mseed"

    return get_test_data(file_name, "MSEED")


@pytest.fixture(scope="session", autouse=True)
def redis():
    return FakeStrictRedis()


@pytest.fixture(scope="session", autouse=True)
def execute_before_any_test():
    os.environ['SPP_COMMON'] = os.path.join(os.getcwd() + '/common')


@pytest.fixture(scope="module", autouse=True)
def settings():
    return sppsettings.settings
