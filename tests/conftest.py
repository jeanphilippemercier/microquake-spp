import os

import pytest

from fakeredis import FakeStrictRedis
import microquake.core.settings as sppsettings


@pytest.fixture(scope="session", autouse=True)
def redis():
    return FakeStrictRedis()


@pytest.fixture(scope="session", autouse=True)
def execute_before_any_test():
    os.environ['SPP_COMMON'] = os.path.join(os.getcwd() + '/common')


@pytest.fixture(scope="module", autouse=True)
def settings():
    return sppsettings.settings
