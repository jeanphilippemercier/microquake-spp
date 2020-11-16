from spp.clients import api_client
from microquake.core.settings import settings
from microquake.core.helpers.time import get_time_zone
from obspy.core import UTCDateTime
from dateutil.parser import parse
from datetime import datetime
import urllib
from microquake.helpers.logging import logger
from importlib import reload
from spp.clients.ims import web_client
from pytz import utc
import requests
import numpy as np

reload(api_client)
reload(web_client)


tz = get_time_zone()

base_url = settings.get('api_base_url')
if base_url[-1] == '/':
    base_url = base_url[:-1]
url = f'{base_url}/events'

start_time = UTCDateTime(datetime(2019, 7, 1, 0, 0, 0, tzinfo=tz))
end_time = UTCDateTime(datetime(2019, 9, 1, 0, 0, 0, tzinfo=tz))

events = api_client.get_catalog(base_url, start_time, end_time,
                                event_type='seismic event', status='accepted')

sites = [station.code for station in settings.inventory.stations()]
base_url_ims = settings.get('ims_base_url')

cat_ims = web_client.get_catalogue(base_url_ims, start_time, end_time, sites,
                                   utc, accepted=True, manual=True)

ims_times = [ev.preferred_origin().time for ev in cat_ims]

for event in events[89:]:
    logger.info(f'processing event {event.event_resource_id}')
    logger.info(f'event time: {event.time_utc}')
    cat = event.get_event()

    time_diff = np.abs(np.array(ims_times) - event.time_utc)

    index = np.argmin(time_diff)

    if time_diff[index] < 5:
        cat[0].magnitudes.append(cat_ims[index].preferred_magnitude())
        cat[0].preferred_magnitude_id = cat[0].magnitudes[-1].resource_id
        cat[0].extra = cat_ims[index]['extra']
        cat[0].origins.append(cat_ims[index].origins[-1])

    # cf = cat[0].CORNER_FREQUENCY
    # energy_s = cat[0].ENERGY_S
    # energy_p = cat[0].ENERGY_P
    # energy = energy_s + energy_p
    # potency_p = cat[0].POTENCY_P
    # potency_s = cat[0].POTENCY_S
    # potency = (potency_p + potency_s) / 2
    # static_stress_drop = cat[0].STATIC_STRESS_DROP
    # apparent_stress = 2 * energy / potency
    #
    # cat[0].magnitudes[0].energy_s_joule = energy_s
    # cat[0].magnitudes[0].energy_p_joule = energy_p
    # cat[0].magnitudes[0].energy_joule = energy_s + energy_p
    #
    # cat[0].magnitudes[0].potency_m3 = potency
    #
    # cat[0].magnitudes[0].static_stress_drop_mpa = static_stress_drop
    # # cat[0].magnitudes[0].apparent_stress_pa = 2 * energy / potency
    #
    # cat[0].magnitudes[0].corner_frequency_hz = cf

    cat[0].preferred_magnitude_id = cat[0].magnitudes[-1].resource_id

    files = api_client.prepare_data(cat=cat.copy())

    patch_url = f'{base_url}/events/{event.event_resource_id}'

    response = requests.patch(patch_url, files=files.copy(),
                              params={'send_to_bus': False})
    # input('coulicou')

# cat =
