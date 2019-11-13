from microquake.clients.ims import web_client
from microquake.clients import api_client
from microquake.core.settings import settings
from datetime import datetime, timedelta
from pytz import utc
from spp.data_connector.pre_processing import pre_process
from microquake.db.connectors import RedisQueue
from microquake.db.models.redis import set_event
from loguru import logger

api_base_url = settings.get('api_base_url')
ims_base_url = settings.get('ims_base_url')

we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE
we_job_queue = RedisQueue(we_message_queue)

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

for i, event in enumerate(sorted_cat):
    logger.info(f'processing event {i} of {len(cat)} -- ({i/len(cat) * 100}%)')
    event_id = event.resource_id.id
    if not api_client.get_event_by_id(api_base_url, event_id):
        logger.info(f'sending event {ct} to the queue')
        set_event(event_id, catalogue=event.copy())
        logger.info(f'sending event with event id {event_id} to the '
                    f'pre_processing queue')
        result = we_job_queue.rq_queue.enqueue(pre_process,
                                               args=(event_id,),
                                               kwargs={'force_send_to_api':
                                                       True,
                                                       'force_send_to_automatic':
                                                       True})
        ct += 1
