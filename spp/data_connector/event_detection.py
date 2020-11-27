from microquake.clients.ims import web_client
from datetime import datetime, timedelta
from microquake.core.settings import settings
import numpy as np
from microquake.helpers.logging import logger
from time import time
from microquake.processors import event_detection
import pandas as pd


def triggering(trace):
    triggers_on, triggers_off = evp.process(trace=trace)

    if not triggers_on:
        return

    sensor = trace.stats.station

    triggers_out = []
    for trigger_on, trigger_off in zip(triggers_on, triggers_off):
        triggers_out.append([sensor, trigger_on, trigger_off])

    return triggers_out


def associate(triggers, window_size=2, hold_off=0, minimum_number_trigger=10):
    df = pd.DataFrame(triggers)

    cts = []
    for t in df['trigger_time']:
        df_tmp = df[(df['trigger_time'] >= t) & (df['trigger_time'] < t + 1)]
        cts.append(len(np.unique(df_tmp['sensor'])))

    df['count'] = cts
    df = df.sort_values('count', ascending=False)

    is_finished = False
    trigger_times = []
    while is_finished is False:
        if df.iloc[0]['count'] > minimum_number_trigger:
            t = df.iloc[0]['trigger_time']
            trigger_times.append(t)
            df = df[(df['trigger_time'] < t - 0.5) |
                    (df['trigger_time'] > t + 1.5)]

            cts = []
            for t in df['trigger_time']:
                df_tmp = df[
                    (df['trigger_time'] >= t) & (df['trigger_time'] < t + 1)]
                cts.append(len(np.unique(df_tmp['sensor'])))

            df['count'] = cts
            df = df.sort_values('count', ascending=False)

            if len(df) == 0:
                is_finished = True

        else:
            is_finished = True

    return trigger_times


end_time = datetime.utcnow() - timedelta(minutes=1)
start_time = end_time - timedelta(minutes=1)

inventory = settings.inventory
network_code = settings.NETWORK_CODE
base_url = settings.IMS_BASE_URL
site_ids = np.sort([int(site.code) for site in inventory.stations()])
sites = [str(site_id) for site_id in site_ids]

logger.info('retrieving the continuous data')
t0 = time()
st = web_client.get_continuous_multiple(base_url, start_time, end_time,
                                        site_ids=site_ids,
                                        network=network_code)
t1 = time()
logger.info(f'done retrieving the continuous data in {t1 - t0:0.0f} second')

cat = web_client.get_catalogue(base_url, start_time, end_time, sites,
                               None, accepted=False, manual=False)

edp = event_detection.Processor()

event_time, sensors = edp.process(stream=st)

# triggers = {'sensor': [],
#             'trigger_time': []}
# for tr in st.composite():
#     try:
#         triggers_on, trigger_off = edp.process(trace=tr)
#     except Exception as e:
#         logger.error(e)
#         continue
#
#     for trigger in triggers_on:
#         triggers['sensor'].append(tr.stats.station)
#         triggers['trigger_time'].append(trigger)
#
# association_times = associate(triggers)
# edp.process(stream=st)

# triggers = {'triggers_on': [],
#             'triggers_off': [],
#             'sensors': [],
#             'channels': []}
# for tr in st.composite():
#     try:
#         trgs = edp.process(trace=tr)
#     except Exception as e:
#         logger.error(e)
#         continue
#     for i in range(0, len(trgs[0])):
#         triggers['triggers_on'].append(trgs[0][i])
#         triggers['triggers_off'].append(trgs[1][i])
#         triggers['sensors'].append(tr.stats.station)
#         triggers['channels'].append(tr.stats.channel)



