
from microquake.IMS import web_api
from multiprocessing import Pool
from microquake.core.data.station import read_stations
import os
import numpy as np

# for scheduling task
import sched, time

import json
from decimal import Decimal
reload(q64)

def run_tmpl(sc, spark_context):
    
    # reading parameters this will allow hot change in the processing parameters while running
    params = json.load(open('ingest_config.json'))

    # schedule the next task
    sc.enter(params['frequency'], 1, run_tmpl,(sc, spark_context))
    
    r = redis.StrictRedis(host=rhost, port=rport, db=rdb)
    site = read_stations('sensors.csv', has_header=True)

    overlap = params['overlap']
    starttime = q64.decode(r.get('endtime')) - timedelta(0, overlap)
    endtime = UTCDateTime.now() - timedelta(0, params['minimum_time_offset'])

    # if time interval is too long, adjust the start time
    window_len = endtime - starttime
    if window_len > params['max_window_length']:
        starttime = endtime - timedelta(0, params['max_window_length'])

    stations = [int(sta) for sta in site.stations(return_code_only=True)]

    # get continuous data for every channel
    mq_map(spark_context, get_data, stations[10:13], params, starttime, endtime)

    r.set('endtime', q64.encode(endtime))
    sc.run()
    return 

def gen_random_stream_test(starttime, endtime, station):
    # this function will need to be changed so it reads some real data for test
    sr = 6000
    npts = int((endtime - starttime) * sr)
    data = np.random.randn(npts)
    stats = Stats()
    tr = Trace(data=data)
    tr.stats.station = station
    tr.stats.sampling_rate = sr
    tr.stats.channel = 'Z'
    tr.stats.starttime = starttime
    return Stream(traces=[tr])


def get_data_site(starttime, endtime, site_ids, UTC=False):
    """
    Get data from a defined start time. The end time is determined by value in the configuration file
    Note that there is no need to define the time zone. The system time defined in the time.json config file will be
    used
    :param starttime: start time local time unless specified by the UTC flag
    :type starttime: datetime.datetime
    :type site_ids: list of int
    :param UTC: if true start and end times are assumed to be expressed in UTC. Local time is assumed otherwise
    (Default False)
    :type UTC: bool
    :return: microquake.core.stream.Stream
    """

    from microquake.IMS.web_api import get_continuous
    from datetime import datetime, timedelta
    from spp import time
    from numpy import arange

    config_dir = os.environ('SPP_CONFIG')
    fname = os.path.join(config_dir, 'ingest_config.json')
    params = json.load(open(fname))
    base_url = params['data_source']['location']

    minimum_offset = params["minimum_time_offset"]
    window_length = params["window_length"]
    overlap = params['overlap']

    starttime = starttime.replace(tzinfo=time.get_time_zone())
    endtime = datetime.now().replace(tzinfo=time.get_time_zone()) - timedelta(seconds=minimum_offset)

    times = arange(starttime, endtime, timedelta(window_length))
    endtime = times[-1] + timedelta(seconds=overlap)

    st = gen_random_stream_test(starttime, endtime, site_ids=site_ids)
    overlap =

    sts = []
    for stime in times[:-1]:
        for etime in times[1:]:
            stime=
            st.trim(starttime=starttime)


    return get_continuous(base_url, start_datetime=starttime, end_datetime=)
    


    if not st:
        return

    stime_s = starttime.strftime("%Y%m%d%H%M%S")
    etime_s = endtime.strftime("%Y%m%d%H%M%S")
    key = '%s_%s_%d' % (stime_s, etime_s, station)

    r.set(key, q64.encode(st), ex=params['data_expiration_time'])

    # return a the key. The keys will be send to a module
    # which will create combination of channels. 
    return key

    # Send message through the messaging system
    # the key will consist 
    # the message will be consumed by a module





s = sched.scheduler(time.time, time.sleep)

params = json.load(open('ingest_config.json'))
base_url = params['data_source']['location']
rhost = params['redis']['host']
rport = params['redis']['port']
rdb = params['redis']['db']
r = redis.StrictRedis(host=rhost, port=rport, db=rdb)

start_delay = params['minimum_time_offset'] + params['frequency']
endtime = UTCDateTime.now() - timedelta(0, start_delay)


r.set("endtime", q64.encode(UTCDateTime(endtime)))

run_tmpl(s, None)





