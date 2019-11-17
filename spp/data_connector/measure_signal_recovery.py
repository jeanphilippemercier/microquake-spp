from microquake.clients.ims import web_client
from microquake.core.settings import settings
from obspy.core import UTCDateTime
from datatime import datetime, timedelta
import numpy as np
from pytz import utc
import requests
import json
from loguru import logger

# inventory = settings.inventory

signal_duration_seconds = 10

starttime = UTCDateTime.utcnow() - 60
endtime = startime - signal_duration_seconds

ims_base_url = settings.get('IMS_BASE_URL')
api_base_url = settings.get('API_BASE_URL')

if api_base_url[-1] == '/':
    api_base_url = api_base_url[:-1]

api_url = api_base_url + '/sensors'
response = requests.get(api_url)

sensors = json.loads(response.content)['results']

network_code = settings.NETWORK_CODE

signal_quality_template = {'energy': 0,
                           'integrity':0,
                           'sampling_rate':0,
                           'num_samples':0,
                           'amplitude':0,
                           'station':0}

for sensor in sensors:
    logger.info(f'Measuring signal quality for sensor {sensor["code"]}')
    signal_quality = signal_quality_template
    # st = web_client.get_continuous(ims_base_url, starttime, endtime,
    #                                [str(sensor['code'])], utc,
    #                                network=network_code)
    st = []

    signal_quality['sensor'] = sensor['code']
    if len(st) == 0:
        requests.patch(api_url + f'/{sensor["id"]}', json=sensor)
        continue

    st.detrend('demean').detrend('linear')
    c = st.composite()
    expected_signal_length = c[0].stats.sampling_rate * signal_duration_seconds
    signal_quality['energy'] = np.var(c[0].data)
    signal_quality['integrity'] = len(c[0].data) / expected_signal_length
    signal_quality['sampling_rate'] = c[0].stats.sampling_rate
    signal_quality['amplitude'] = np.max(np.abs(c[0].data))

    sensor['signal_quality'] = signal_quality

    requests.patch(api_url + f'/{sensor["id"]}', json=sensor)
    input('babou')

