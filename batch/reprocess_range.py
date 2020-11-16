from spp.clients.api_client import (get_events_catalog,
                                           put_data_from_objects)
from microquake.core.settings import settings
from from microquake.helpers.time import get_time_zone
from urllib.parse import urlencode
import requests

tz = get_time_zone()

# months = ['04', '05', '06', '07', '08', '09', '10', '11']

# for month in ['04', '05', '06', '07', '08', '09', '10', '11']:
query = urlencode({'time_utc_after': f'2018-01-01T00:00:00Z',
                   'time_utc_before': f'2019-02-01T00:00:00Z',
                   'event_type': 'earthquake',
                   'status': 'accepted'})

base_url = settings.get('api_base_url')
url = base_url + 'events'
r = requests.patch(f'{url}?{query}', json={'send_to_bus': True})
print(r)

# re = get_events_catalog(base_url, starttime, endtime, event_type='earthquake')
#
# for event in re:
#     logger.info(f'processing event {event.event_resource_id}')
#     logger.info('getting the event catalogue')
#     cat = event.get_event().copy()
#     logger.info('getting the waveforms')
#     fixed_length = event.get_waveforms()
#
#     cat_out = magnitude_meta_processor(cat, fixed_length)
#     put_data_from_objects(base_url, event.network, cat=cat_out.copy())



