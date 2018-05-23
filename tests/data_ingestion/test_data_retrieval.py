
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
import requests
import calendar
from datetime import datetime
from io import BytesIO
from gzip import GzipFile
import struct
import numpy as np
from microquake.core import Trace, Stats, Stream, UTCDateTime
from timeit import timeit
import requests


def build_request(base_url, start_datetime, end_datetime,
                   site_ids, format='binary-gz', network=''):

    if isinstance(site_ids, int):
        site_ids = [site_ids]

    start_datetime_utc = UTCDateTime(start_datetime)
    end_datetime_utc = UTCDateTime(end_datetime)

    time_start = calendar.timegm(start_datetime_utc.timetuple()) * 1e9 + start_datetime_utc.microsecond * 1e3 - 1e9
    time_end = calendar.timegm(end_datetime_utc.timetuple()) * 1e9 + end_datetime_utc.microsecond * 1e3 + 1e9
    url_cont = base_url + '/continuous-seismogram?' + \
               'startTimeNanos=%d&endTimeNanos=%d&siteId' + \
               '=%d&format=%s'

    urls = []
    for site in site_ids:
        url = url_cont % (time_start, time_end, site, format)
        url = url.replace('//', '/').replace('http:/', 'http://')
        urls.append(url)
    return urls


config_dir = os.environ['SPP_CONFIG']
fname = os.path.join(config_dir, 'ingest_config.yaml')
with open(fname) as cfg_file:
    params = yaml.load(cfg_file)
    params = params['data_ingestion']

base_url = params['data_source']['location']

minimum_offset = params["minimum_time_offset"]
window_length = params["window_length"]
overlap = timedelta(seconds=2 * params['overlap'])
filter = params['filter']
taper = params['taper']

time_zone = time.get_time_zone()
starttime = datetime.now().replace(tzinfo=time_zone) - timedelta(minutes=10)
# starttime = q64.encode(starttime)
endtime = starttime + timedelta(seconds=minimum_offset)

common_dir = os.environ['SPP_COMMON']
station_file = os.path.join(common_dir, 'sensors.csv')
site = read_stations(station_file, has_header=True)

st_code = [int(station.code) for station in site.stations()]

urls = build_request(base_url, starttime, endtime, site_ids=st_code)
url = urls[20]

# %timeit requests.get(url, stream=True)

# r = requests.get(url, stream=True)


