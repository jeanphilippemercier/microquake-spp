from microquake.clients.ims import web_client
from microquake.helpers.timescale_db import get_continuous_data, get_db_lag
from microquake.core.settings import settings
from obspy.core import UTCDateTime
from datetime import datetime, timedelta
import numpy as np
import requests
from microquake.helpers.logging import logger
from time import time

# inventory = settings.inventory

signal_duration_seconds = 60

endtime = datetime.utcnow() - timedelta(minutes=2)
starttime = endtime - timedelta(seconds=signal_duration_seconds)

ims_base_url = settings.get('IMS_BASE_URL')
api_base_url = settings.get('API_BASE_URL')

if api_base_url[-1] == '/':
    api_base_url = api_base_url[:-1]

# api_url_sensors = api_base_url + '/inventory/sensors?page_size=1000'
api_url = api_base_url + '/signal_quality'
# response = requests.get(api_url_sensors)

inventory = settings.inventory

network_code = settings.NETWORK_CODE

signal_quality_template = {'energy': '0',
                           'integrity': '0',
                           'sampling_rate': '0',
                           'num_samples': '0',
                           'amplitude': '0'}

sensor_ids = np.sort([int(station.code) for station in inventory.stations()])

logger.info('retrieving the continuous data')

t0 = time()
st_all = web_client.get_continuous_multiple(ims_base_url, starttime, endtime,
                                            site_ids=sensor_ids,
                                            network=network_code)
t1 = time()
logger.info(f'done retrieving the continuous data in {t0 - t1:0.0f} seconds')

for sensor_code in sensor_ids:
    sensor_code = str(sensor_code)
    logger.info(f'Measuring signal quality for sensor {sensor_code}')
    st = st_all.select(station=sensor_code)

    signal_quality = signal_quality_template.copy()

    signal_quality['sensor_code'] = sensor_code
    if len(st) == 0:
        logger.info(f'signal recovery is 0 %')
        r = requests.post(api_url, json=signal_quality)

        if r:
            logger.info(f'successfully posted to the API')
        else:
            logger.info(f'post failed, the API responded with code '
                        f'{r.status_code}')
        continue

    try:
        st = st.detrend('demean').detrend('linear')
    except ValueError as e:
        logger.error(e)
        logger.info('replacing the NaN by zero')
        for i, tr in enumerate(st):
            st[i].data = np.nan_to_num(st[i].data)
        st = st.detrend('demean').detrend('linear')
    c = st.composite()

    expected_signal_length = c[0].stats.sampling_rate * signal_duration_seconds
    integrity = len(c[0].data) / expected_signal_length

    signal_quality['energy'] = str(np.std(c[0].data))
    signal_quality['integrity'] = str(integrity)
    signal_quality['sampling_rate'] = str(c[0].stats.sampling_rate)
    signal_quality['amplitude'] = str(np.max(np.abs(c[0].data)))
    signal_quality['num_sample'] = str(len(c[0]))

    integrity = int(float(signal_quality['integrity']) * 100)
    logger.info(f'signal recovery is {integrity} %')

    try:
        r = requests.post(api_url, json=signal_quality)
    except requests.exceptions.ConnectionError as r:
        logger.error(f'connection error when attempting to POST information '
                     f'for sensor {sensor_code}')
        continue

    if r:
        logger.info(f'successfully posted to the API')
    else:
        logger.info(f'post failed, the API responded with code '
                    f'{r.status_code}')

