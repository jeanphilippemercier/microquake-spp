from spp.utils.config import Configuration
from microquake.nlloc import NLL
from microquake.core.event import read_events
from microquake.core import UTCDateTime
from microquake.IMS import web_client

conf = Configuration()
settings = conf.settings

# initialize NLL object

tz = conf.get_time_zone()
site = conf.get_stations()
base_url = conf.settings.data_connector.path
end_time = UTCDateTime.now()
start_time = end_time - 10 * 3600

# cat = web_client.get_catalogue(base_url, start_time, end_time, site, tz,
#                                get_arrivals=True)

cat = read_events('test.xml', format='QUAKEML')


# def get_catalogue(base_url, start_datetime, end_datetime, site,
#                   timezone, blast=True, event=True, accepted=True, manual=True,
#                   get_arrivals=False):


project_code = settings.project_code
base_folder = settings.nlloc.nll_base
gridpar = conf.nll_velgrids()
sensors = conf.nll_sensors()
params = conf.settings.nlloc

# cat = read_events('event_synthetic.xml', format='QUAKEML')

nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
          sensors=sensors, params=params)

cat_out = nll.run_event(cat[-1].copy())



