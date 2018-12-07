import colorama
colorama.init(convert=True)

from microquake.IMS import web_client
from microquake.core import UTCDateTime
from spp.utils.application import Application
import pytz
import os
from io import BytesIO
from spp.utils import seismic_client
from datetime import datetime


app = Application()
site = app.get_stations()
base_url = app.settings.data_connector.path
endtime = UTCDateTime.now()
starttime = endtime - 2.5 * 3600 * 24
starttime = UTCDateTime(2018, 5, 17, 13, 45) # UTCDateTime(2018, 5, 1)# UTCDateTime.now()
tz = app.get_time_zone()

endtime = endtime.datetime.replace(tzinfo=pytz.utc)
starttime = starttime.datetime.replace(tzinfo=pytz.utc)

cat = web_client.get_catalogue(base_url, starttime, endtime, site, tz,
                               blast=False)
site_ids = [int(station.code) for station in site.stations()]

print(cat)

api_base_url = app.settings.seismic_api.base_url

for evt in cat:
    print(evt)
    evt = web_client.get_picks_event(base_url, evt, site, tz)
    seis = web_client.get_seismogram_event(base_url, evt, 'OT', tz)

    event_id = evt.resource_id.id
    st_io = BytesIO()
    seis.write(st_io, format='MSEED')
    mseed_bytes = st_io.getvalue()
    ev_io = BytesIO()
    evt.write(ev_io, format='QUAKEML')
    event_bytes = ev_io.getvalue()

    data, files = seismic_client.build_request_data_from_bytes(event_id, event_bytes, 
                                                               mseed_bytes, None)

    seismic_client.post_event_data(api_base_url, data, files)

    # time = evt.preferred_origin().time
    # stime = (time - 10).datetime.replace(tzinfo=pytz.utc)
    # etime = (time + 10).datetime.replace(tzinfo=pytz.utc)
    # st_20s = web_client.get_continuous(base_url, stime, etime, site_ids, tz)

    # fname = str(time).replace('-','_').replace(':','_')

    # seis.write(fname + '.mseed', format='MSEED')
    # st_20s.write(fname + '_20s.mseed', format='MSEED')

    # evt.write(fname + '.xml', format='QUAKEML')