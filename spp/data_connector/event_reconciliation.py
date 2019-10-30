from microquake.clients.ims import web_client
from microquake.clients import api_client
from microquake.core.settings import settings
from datetime import datetime, timedelta
from pytz import utc
from.pre_processing import pre_process
from microquake.db.models.redis import set_event


api_base_url = settings.get('api_base_url')
ims_base_url = settings.get('ims_base_url')

# will look at the last day of data
end_time = datetime.utcnow() - timedelta(hours=1)
start_time = end_time - timedelta(hours=24)
inventory = settings.inventory

cat = web_client.get_catalogue()

cat = web_client.get_catalogue(ims_base_url, start_time, end_time, inventory,
                               utc, blast=True, event=True, accepted=True,
                               manual=True, get_arrivals=False)

for event in cat:
    event_id = event.resource_id.id
    if api_client.get_event_by_id(api_base_url, event_id):
        set_event(event_id, catalogue=event.copy())
        pre_process(event_id, force_send_to_api=True,
                    force_send_to_automatic=True)
