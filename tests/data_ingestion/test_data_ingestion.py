from microquake.core import read_stations
from datetime import datetime, timedelta
from spp import time
import os
from pathos.multiprocessing import Pool
import yaml
from microquake.core.util import q64
from toolz.functoolz import curry
from timeit import default_timer as timer
from IPython.core.debugger import Tracer


@curry
def get_data(base_url, starttime, endtime, overlap, window_length, filter, taper, site_id):

    print site_id
    from numpy import floor
    from microquake.core import UTCDateTime
    from microquake.IMS import web_api
    from datetime import timedelta
    reload(web_api)
    from IPython.core.debugger import Tracer
    # starttime = q64.decode(starttime_q64)
    # endtime = q64.decode(endtime_q64)
    # overlap = q64.decode(overlap_q64)

    period = endtime - starttime
    nsec = int(floor(period.total_seconds()))
    dts = range(0, nsec + 1, window_length)
    dtimes = [timedelta(seconds=dt) for dt in dts]

    # starttime = UTCDateTime(starttime)
    # endtime = UTCDateTime(endtime)
    starttime = starttime - overlap
    endtime = endtime + overlap
    # st = web_api.get_continuous(base_url, starttime, endtime, site_ids=[site_id])
    try:
        st = web_api.get_continuous(base_url, starttime, endtime, site_ids=[site_id])
    except Exception as e:
        print(e)
        return

    sts = []
    # for stime in times[:-1]:
    #     for etime in times[1:]:
    for dtime in dtimes:
        stime = starttime + dtime
        etime = starttime + dtime + timedelta(seconds=window_length) + 2 * overlap
        st_trim = st.copy().trim(starttime=UTCDateTime(stime), endtime=UTCDateTime(etime))
        st_trim = st_trim.taper(1, **taper)
        st_trim = st_trim.filter(**filter)
        sts.append(st_trim)

    return (sts, 0)


if __name__ == '__main__':

    config_dir = os.environ['SPP_CONFIG']
    fname = os.path.join(config_dir, 'ingest_config.yaml')
    with open(fname) as cfg_file:
        params = yaml.load(cfg_file)
        params = params['data_ingestion']

    base_url = params['data_source']['location']

    minimum_offset = params["minimum_time_offset"]
    window_length = params["window_length"]
    overlap = timedelta(seconds=2*params['overlap'])
    filter = params['filter']
    taper = params['taper']

    time_zone = time.get_time_zone()
    starttime = datetime.now().replace(tzinfo=time_zone) - timedelta(minutes=10)
    # starttime = q64.encode(starttime)
    endtime = starttime + timedelta(seconds=minimum_offset)
    # endtime = q64.encode(endtime)

    # overlap = q64.encode(overlap)

    station_file = os.path.join(config_dir, 'sensors.csv')
    site = read_stations(station_file, has_header=True)

    st_code = [int(station.code) for station in site.stations()]

    p = Pool(8)

    starttime_q64 = q64.encode(starttime)
    endtime_q64 = q64.encode(endtime)
    overlap_q64 = q64.encode(overlap)

    # get_data_f = lambda k: get_data(k, base_url, starttime, endtime, overlap)

    #get_data(32, base_url, starttime, endtime, overlap)

    start = timer()
    results = p.map(get_data(base_url, starttime, endtime, overlap, window_length, filter, taper), st_code)
    end = timer()
    print(end - start)

    outputs = [result[0] for result in results]


    # st = get_data(base_url, starttime, endtime, overlap, window_length, filter, taper).call(75)




