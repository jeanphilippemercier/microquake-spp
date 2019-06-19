import os
import sys

import pytest

import spp.core.settings as sppsettings

sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))


@pytest.fixture(scope="session", autouse=True)
def execute_before_any_test():
    os.environ['SPP_COMMON'] = os.path.join(os.getcwd() + '/common')


@pytest.fixture(scope="module", autouse=True)
def settings():
    return sppsettings.settings
