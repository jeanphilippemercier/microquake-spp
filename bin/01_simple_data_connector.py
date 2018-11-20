#!/usr/bin/env python3

# Simple data connector. This data connector does not superseeds the data
# connector previously written. This data connector will be run on a local
# machine on the Oyu Tolgoi site. It will simply get data around manually
# processed events. This script will be scheduled to run every few minutes
# and will send data both the the seismic processing platform.

from microquake.IMS import web_client
from microquake.core import UTCDateTime
from spp.utils import get_data_connector_parameters, get_stations
from microquake.core import Stream
from spp.utils.application import Application

time_windows = 10 * 60 * 60 # window length for time request in hours

# request the data from the IMS system

end_time = UTCDateTime.now() - 2 * 60
start_time = end_time - 10 * 60 * 60  # looking at the past 10 hours

app = Application()
base_url = app.settings.data_connector.path
site = app.get_stations()
tz = app.get_time_zone()

cat = web_client.get_catalogue(base_url, start_time, end_time, site, tz)

stations = [station.code for station in site.stations()]

for evt in cat:
    stime = evt.preferred_origin().time - 5
    etime = evt.preferred_origin().time + 5

    tmp = web_client.get_continuous(base_url, stime, etime, stations)
    st = Stream()
    for tr in tmp:
        if len(tr.data) == 0:
            continue
        st.traces.append(tr)

    st_io = BytesIO()
    st.write(st_io, format='MSEED')
    # send the data to the seismic-api endpoint on Azure @Hanee, could you
    # please complete the code required to push the mseed to the server

    # CODE TO SEND THE DATA TO THE END POINT HERE



    # decomposed_mseed = mseed_decomposer(st)
    #
    # write_decomposed_mseed_to_kafka(decomposed_mseed)

