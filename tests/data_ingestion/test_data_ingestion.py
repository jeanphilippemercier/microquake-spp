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

    print(site_id)
    from numpy import floor
    from microquake.core import UTCDateTime
    from microquake.IMS import web_api
    from datetime import timedelta
    from importlib import reload
    from dateutil import parser

    reload(web_api)
    import pdb

    period = endtime - starttime
    nsec = int(floor(period.total_seconds()))
    dts = range(0, nsec + 1, window_length)
    dtimes = [timedelta(seconds=dt) for dt in dts]

    starttime = starttime - overlap
    endtime = endtime + overlap
    try:
        st = web_api.get_continuous(base_url, starttime, endtime, site_ids=[site_id])
    except Exception as e:
        print(e)
        return

    if not st:
        return

    sts = []

    for dtime in dtimes:
        stime = starttime + dtime
        etime = starttime + dtime + timedelta(seconds=window_length) + 2 * overlap
        st_trim = st.copy().trim(starttime=UTCDateTime(stime), endtime=UTCDateTime(etime))
        st_trim = st_trim.taper(1, **taper)
        st_trim = st_trim.filter(**filter)
        sts.append(st_trim)

    return sts


if __name__ == '__main__':

    config_dir = os.environ['SPP_CONFIG']
    common_dir = os.environ['SPP_COMMON']
    fname = os.path.join(config_dir, 'ingest_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['data_ingestion']


    base_url = params['data_source']['location']

    minimum_offset = params["minimum_time_offset"]
    window_length = params["window_length"]
    overlap = timedelta(seconds=params['overlap'])
    period = params['period']
    filter = params['filter']
    taper = params['taper']
    max_window_length = params['max_window_length']

    time_zone = time.get_time_zone()

    # get end time of last window, note that time in the file should be in
    # local time
    ftime = os.path.join(common_dir, 'ingest_info.txt')

    now = datetime.now().replace(tzinfo=time_zone)

    if not os.path.isfile(ftime):
        starttime = now \
                    - timedelta(seconds=minimum_offset) \
                    - timedelta(seconds=period) \
                    - overlap
        endtime = now - timedelta(seconds=minimum_offset)

    else:
        with open(ftime, 'r') as timefile:
            starttime = parser.parse(timefile.readline()) - overlap
            starttime = starttime.replace(time_zone=time_zone)

            dt = (now - starttime).total_seconds()
            if dt - minimum_offset > max_window_length:
                starttime = now - timedelta(seconds=(minimum_offset +
                                                    max_window_length)) - \
                            overlap

            endtime = now - timedelta(seconds=minimum_offset)


    # endtime = starttime + timedelta(seconds=window_length)


    station_file = os.path.join(common_dir, 'sensors.csv')
    site = read_stations(station_file, has_header=True)

    st_code = [int(station.code) for station in site.stations()]

    p = Pool(10)
    #
    start = timer()
    results = p.map(get_data(base_url, starttime, endtime, overlap,
                             window_length, filter, taper), st_code)


    end = timer()

    for result in results:
        result[0]

    print(end - start)

    with open(ftime, 'w') as timefile:
        timefile.write(endtime.strftime("%Y-%m-%d %H:%M:%S.%f"))


    for result in results:








