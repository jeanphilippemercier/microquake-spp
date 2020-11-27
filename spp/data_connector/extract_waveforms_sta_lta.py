from time import time

from pytz import utc

from microquake.helpers.logging import logger
from microquake.clients.ims import web_client
from microquake.core.settings import settings
from obspy.signal.trigger import recursive_sta_lta

sites = [int(station.code) for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')
api_base_url = settings.get('api_base_url')
inventory = settings.inventory
network_code = settings.NETWORK_CODE

use_time_scale = settings.USE_TIMESCALE

# tolerance for how many trace are not recovered
minimum_recovery_fraction = settings.get(
    'data_connector').minimum_recovery_fraction


def extract_continuous(starttime, endtime, sensor_id):
    """
    Extracts the continuous trace for only one sensor
    :param starttime: start time of the trace
    :param endtime:  end time of the trace
    :param sensor_id: sensor id
    :return:
    """
    s_time = time()

    st = web_client.get_continuous(base_url, starttime, endtime,
                                   [str(sensor_id)], network=network_code)

    st = st.detrend('demean').detrend('linear')

    return st


def trigger(composite_trace, sta, lta):
    """
    :param composite_trace: a trace object, ideally composite
    :param sta: length of the window used to calculate the short term average
    :param lta: length of the window used to calculate the long term average
    :return: the triggers for the trace
    """
    st = st.composite()
    station = composite_trace().stats.station
    inv_station = inventory.select(station)
    st = st.filter('highpass', freq=15)
