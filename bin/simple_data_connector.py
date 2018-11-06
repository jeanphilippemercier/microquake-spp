# Simple data connector. This data connector does not superseeds the data
# connector previously written. This data connector will be run on a local
# machine on the Oyu Tolgoi site. It will simply get data around manually
# processed events. This script will be scheduled to run every few minutes
# and will send data both the the seismic processing platform.

from microquake.IMS import web_client
from microquake.core import UTCDateTime
from spp.utils import get_data_connector_parameters, get_stations
from spp.data_connector import write_mseed_chunk_to_kafka
from spp.time import get_time_zone
from toolz.functoolz import curry
from microquake.core import Stream
from io import BytesIO

# request the data from the IMS system

end_time = UTCDateTime.now() - 2 * 60
start_time = end_time - 10 * 60 * 60

dc_params = get_data_connector_parameters()

base_url = dc_params['data_source']['location']
site = get_stations()
tz = get_time_zone()

cat = web_client.get_catalogue(base_url, start_time, end_time, site, tz)

stations = [station.code for station in site.stations()]

workers = dc_params['multiprocessing']['workers']

for evt in cat:
    stime = evt.preferred_origin().time - 5
    etime = evt.preferred_origin().time + 5

    tmp = web_client.get_continuous(base_url, stime, etime, stations)
    sts = Stream()
    for tr in tmp:
        if len(tr.data) == 0:
            continue
        sts.traces.append(tr)
        # if tr.data == nan:
        #     continue
        # sts.traces.append(tr)
    # map_results = p.map(get_continuous(base_url, stime, etime), stations)

    out_bytes = BytesIO()
    sts.write(out_bytes, format='MSEED')

    # write_mseed_chunk_to_kafka(sts)


    # st = web_api.get_continuous(base_url, stime, etime, stations)


