# This function should receive a time as input. This time should correspond
# to the center of the time window. The current function is only a placeholder
# for future development.

from microquake.core.event import Catalog
from microquake.core import read_events
cat = read_events('9e63e1141c405572afb178627f3fc990.xml')
cat
cat[0].preferred_origin().time = cat[0].preferred_origin().time + 2
from spp.db.models.redis import set_event
cat[0].preferred_origin().time = cat[0].preferred_origin().time - 1
cat[0].preferred_origin().time
from microquake.core.settings import settiings
from microquake.core.settings import settings
we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE
we_message_queue
from datetime import datetime, timedelta
from importlib import reload
from time import sleep, time

import signal

import sqlalchemy as db
from pytz import utc

from microquake.helpers.logging import logger
from spp.clients.ims import web_client
from microquake.helpers.time import get_time_zone
from microquake.core.settings import settings
from spp.db.connectors import (RedisQueue, connect_postgres,
                                      record_processing_logs_pg)
from spp.db.models.alchemy import processing_logs
from spp.db.models.redis import set_event
from obspy.core.event import ResourceIdentifier
from spp.data_connector import pre_processing
from spp.data_connector.pre_processing import pre_process
from requests.exceptions import RequestException
import os
we_message_queue = settings.PRE_PROCESSING_MESSAGE_QUEUE
we_job_queue = RedisQueue(we_message_queue)
we_job_queue_low_priority = RedisQueue(we_message_queue + '.low_priority')
cat[0].resource_id
cat[0].resource_id = ResourceIdentifier()
cat[0].resource_id
event_id = cat[0].resource_id
set_event(event_id, catalogue=event.copy())
event = cat[0]
set_event(event_id, catalogue=event.copy())
event_id = cat[0].resource_id.id
set_event(event_id, catalogue=event.copy())
result = we_job_queue.submit_task(pre_process, event_id=event_id)
result