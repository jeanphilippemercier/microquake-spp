from microquake.IMS.web_api import get_continuous
from microquake.core import read_stations
from datetime import datetime, timedelta
from spp import time
from numpy import arange
import os
from multiprocessing import Pool
import json
from numpy import floor
from microquake.core.util import q64
from microquake.core import UTCDateTime


def get_data(site_id, base_url, starttime, endtime, overlap):

    print site_id
    period = endtime - starttime
    nsec = int(floor(period.total_seconds()))
    dts = range(0, nsec, window_length)
    times = [starttime + timedelta(seconds=dt) + overlap for dt in dts]

    starttime = UTCDateTime(starttime)
    endtime = UTCDateTime(endtime)
    overlap = overlap
    st = get_continuous(base_url, starttime, endtime, site_ids=[site_id])

    sts = []
    for stime in times[:-1]:
        for etime in times[1:]:
            sts.append(st.trim(starttime=UTCDateTime(stime), endtime=UTCDateTime(etime + overlap)))

    return sts


if __name__ == '__main__':

    config_dir = os.environ['SPP_CONFIG']
    fname = os.path.join(config_dir, 'ingest_config.json')
    params = json.load(open(fname))
    base_url = params['data_source']['location']

    minimum_offset = params["minimum_time_offset"]
    window_length = params["window_length"]
    overlap = timedelta(seconds=2*params['overlap'])

    time_zone = time.get_time_zone()
    starttime = datetime.now().replace(tzinfo=time_zone) - timedelta(seconds=180)
    # starttime = q64.encode(starttime)
    endtime = datetime.now().replace(tzinfo=time_zone) - timedelta(seconds=minimum_offset)
    # endtime = q64.encode(endtime)

    # overlap = q64.encode(overlap)

    station_file = os.path.join(config_dir, 'sensors.csv')
    site = read_stations(station_file, has_header=True)

    st_code = [int(station.code) for station in site.stations()]

    p = Pool(8)
    get_data_f = lambda k: get_data(k, base_url, starttime, endtime, overlap)
    st_stations = map(get_data_f, st_code)






