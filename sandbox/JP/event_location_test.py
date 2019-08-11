from spp.utils.application import Application
from microquake.nlloc import NLL
from microquake.core.event import read_events
from microquake.core import UTCDateTime
from microquake.clients.ims import web_client

# msgpack.unpackb(packed_dict, object_hook=decode_datetime, raw=False)

conf = Application()
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

site_ids = [station.code for station in site.stations()]

for event in cat:
    start = event.preferred_origin().time - 1
    end = event.preferred_origin().time + 1
    st = web_client.get_continuous(base_url, start, end,
                                   site_ids)

    ev_time = event.preferred_origin().time

    st.write('%s.mseed' % ev_time, format='MSEED')



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



