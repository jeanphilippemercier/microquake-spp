from spp.clients.ims import web_client
from spp.clients import api_client
from microquake.core.settings import settings
from datetime import datetime, timedelta
from pytz import utc
from spp.data_connector.pre_processing import pre_process
from spp.db.connectors import RedisQueue
from spp.db.models.redis import set_event
from spp.core.helpers.logging import logger
import requests
import urllib

api_base_url = settings.get('api_base_url')
ims_base_url = settings.get('ims_base_url')

we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE
we_job_queue = RedisQueue(we_message_queue)
we_job_queue_low_priority = RedisQueue(we_message_queue + '.low_priority')

# will look at the last day of data
end_time = datetime.utcnow() - timedelta(hours=1)

reconciliation_period = settings.get('reconciliation_period_days')

# looking at the events for the last month
start_time = end_time - timedelta(days=reconciliation_period)
inventory = settings.inventory

cat = web_client.get_catalogue(ims_base_url, start_time, end_time, inventory,
                               utc, blast=True, event=True, accepted=True,
                               manual=True, get_arrivals=False)
ct = 0

sorted_cat = sorted(cat, reverse=True,
                    key=lambda x: x.preferred_origin().time)

api_username = settings.get('API_USERNAME')
api_password = settings.get('API_PASSWORD')

sc = api_client.SeismicClient(api_base_url,
                              username=api_username,
                              password=api_password)

for i, event in enumerate(sorted_cat):
    logger.info(f'processing event ({event.resource_id} '
                f'{i+1} of {len(cat)} -- '
                f'({(i + 1)/len(cat) * 100}%)')

    event_time = event.preferred_origin().time
    tolerance = 0.5

    s_time = event_time - tolerance
    e_time = event_time + tolerance

    response, re_list = sc.events_list(s_time, e_time, status='accepted')

    if re_list:
        continue

    event_id = event.resource_id.id
    response, event2 = sc.events_read(event_id)

    if event2:
        if event2.evaluation_mode == 'manual':
            continue
        else:
            body = sc.event_detail()

        response, re_list = sc.events_list(s_time, e_time,
                                           status='rejected',
                                           evaluation_mode='automatic')
        if re_list:
            sc.events_partial_update(event_id, status='preliminary')
    else:
        logger.info(f'sending event {ct} to the queue')
        set_event(event_id, catalogue=event.copy())
        logger.info(f'sending event with event id {event_id} to the '
                    f'pre_processing queue')
        result = we_job_queue.rq_queue.enqueue(pre_process,
                                               args=(event_id,),
                                               kwargs={'force_send_to_api':
                                                           True,
                                                       'force_send_to_automatic':
                                                           True,
                                                       'force_accept':
                                                           True})

    ct += 1
