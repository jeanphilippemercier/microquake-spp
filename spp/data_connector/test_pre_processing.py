import os
os.environ["SPP_USE_TIMESCALE"] = 'false'

from spp.clients.ims import web_client
from microquake.core.settings import settings
from datetime import datetime, timedelta
from microquake.helpers.time import get_time_zone
from pytz import utc
from microquake.db.models.redis import set_event, get_event
from spp.data_connector.pre_processing import pre_process, send_to_api

sites = [station.code for station in settings.inventory.stations()]
base_url = settings.get('ims_base_url')

tz = get_time_zone()

endtime = datetime.utcnow().replace(tzinfo=utc) - \
              timedelta(seconds=60)

starttime = endtime - timedelta(minutes=300)
cat = web_client.get_catalogue(base_url, starttime, endtime, sites,
                               utc, accepted=False, manual=False)

set_event('test', catalogue=cat[0])

pre_process('test')

send_to_api('test')
