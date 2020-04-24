from microquake.clients.api_client import RequestEvent
from microquake.core.settings import settings
import requests
from loguru import logger
from tqdm import tqdm
from pandas import DataFrame
import numpy as np

inventory = settings.inventory

query = 'https://api.microquake.org/api/v1/events?' \
        'event_type=earthquake&status=accepted'

events = []
while query:
    re = requests.get(query)
    # from ipdb import set_trace; set_trace()()
    if not re:
        break
    response = re.json()
    logger.info(f"page {response['current_page']} of "
                f"{response['total_pages']}")

    query = response['next']

    for event in response['results']:
        events.append(RequestEvent(event))

for event in tqdm(events):

    st = event.get_waveforms().detrend('demean')
    cat = event.get_event()
    ev_loc = cat[0].preferred_origin().loc
    origin = cat[0].preferred_origin()

    ppv_list = {'event id': [],
                'sensor id': [],
                'event time epoch': [],
                'event time utc': [],
                'event x': [],
                'event y': [],
                'event z': [],
                'sensor x': [],
                'sensor y': [],
                'sensor z': [],
                'euclidien distance (m)': [],
                'distance (m)': [],
                'distance p-ray (m)': [],
                'distance s-ray (m)': [],
                'ppv (m/s)': [],
                'back-azimuth': [],
                'take-off angle': []}
    for tr in st.composite():
        tr = tr.detrend('demean')
        sensor_code = tr.stats.station
        sensor = inventory.select(str(sensor_code))
        try:
            ray_p = origin.get_ray_station_phase(sensor, 'P')
            ray_s = origin.get_ray_station_phase(sensor, 'S')
        except Exception as e:
            logger.error(e)
            ray_p = None
            ray_s = None

        euclidien_distance = np.linalg.norm(sensor.loc - ev_loc)
        ppv_list['event id'].append(event.event_resource_id)
        ppv_list['sensor id'].append(sensor_code)
        ppv_list['event time epoch'].append(event.time_epoch)
        ppv_list['event time utc'].append(event.time_utc)
        ppv_list['event x'].append(event.x)
        ppv_list['event y'].append(event.y)
        ppv_list['event z'].append(event.z)
        ppv_list['sensor x'].append(sensor.x)
        ppv_list['sensor y'].append(sensor.y)
        ppv_list['sensor z'].append(sensor.z)
        ppv_list['euclidien distance (m)'].append(euclidien_distance)
        if ray_p is not None:
            ppv_list['distance p-ray (m)'].append(ray_p.length)
            ppv_list['distance (m)'].append(ray_p.length)
            ppv_list['back-azimuth'].append(ray_p.baz)
            ppv_list['take-off angle'].append(ray_p.takeoff_angle)
        else:
            ppv_list['distance p-ray (m)'].append(0)
            ppv_list['distance (m)'].append(euclidien_distance)
            ppv_list['back-azimuth'].append(0)
            ppv_list['take-off angle'].append(0)

        if ray_s is not None:
            ppv_list['distance s-ray (m)'].append(ray_s.length)
        else:
            ppv_list['distance s-ray (m)'].append(0)

        if sensor.motion == 'ACCELERATION':
            tr = tr.integrate()
        max = tr.data.max()
        min = tr.data.min()
        ppv = (max - min) / 2
        ppv_list['ppv (m/s)'].append(ppv)

    df = DataFrame(ppv_list)
    df.to_csv(('PPVs.csv'))





