from microquake.core import read_stations
from datetime import datetime, timedelta
from spp import time
import os
from pathos.multiprocessing import Pool
import yaml
from toolz.functoolz import curry
from timeit import default_timer as timer
from IPython.core.debugger import Tracer
import itertools
from dateutil import parser


# class RequestHandler():
#
#     def __init__(self):
#         self.__time_zone = time.get_time_zone()
#         self.__config_dir = os.environ['SPP_CONFIG']
#         self.__common_dir = os.environ['SPP_COMMON']
#         self.__time_info_file = os.path.join(common_dir, 'ingest_info.txt')
#         self.__config_file = os.path.join(config_dir, 'ingest_config.yaml')
#         with open(self.__config_file, 'r') as cfg_file:
#             params = yaml.load(cfg_file)
#             self.__params = params['data_ingestion']
#
#         self.base_url = params['data_source']['location']
#
#         self.minimum_offset = timedelta(seconds=params["minimum_time_offset"])
#         self.window_length = params["window_length"]
#         self.overlap = timedelta(seconds=params['overlap'])
#         self.period = timedelta(seconds=params['period'])
#         self.filter = params['filter']
#         self.taper = params['taper']
#         self.max_window_length = timedelta(seconds=params['max_window_length'])
#
#         now = datetime.now().replace(tzinfo=self.__time_zone)
#
#         if not os.path.isfile(ftime):
#             self.starttime = now \
#                             - self.minimum_offset \
#                             - self.period
#
#             self.endtime = now - self.minimum_offset
#
#         else:
#             with open(self.__time_info_file) as time_file:
#                 self.starttime = parser.parse(time_file.readline()) \
#                                  - self.overlap
#                 self.starttime = self.starttime.replace(time_zone=time_zone)
#
#                 dt = (now - self.starttime).total_seconds()
#                 if dt - self.minimum_offset > self.max_window_length:
#                     self.starttime = now \
#                                      - self.minimum_offset \
#                                      + self.max_window_length \
#                                      - self.overlap
#
#         self.__station_file = os.path.join(common_dir, 'sensors.csv')
#         self.site = read_stations(self.__station_file, has_header=True)
#
#
#     def partition(mapped_values):
#         import collections
#
#         partitionned_data = collections.defaultdict(list)
#         for ele in mapped_values:
#             if not ele:
#                 continue
#             for key, value in ele:
#                 partitionned_data[key].append(value)
#
#         return partitionned_data.values()
#
#     def reduce(partitionned_data):
#         from microquake.core.stream import Stream
#
#         datas = partitionned_data
#
#         traces = []
#         for data in datas:
#             for tr in data:
#                 traces.append(tr)
#
#         return Stream(traces=traces)



# Create Local get_continous to load data from files:
def get_continuous_local(base_url, start_datetime, end_datetime,
                   site_ids, format='binary-gz', network=''):
    """
    :param base_url: base url of the IMS server
    example: http://10.95.64.12:8002/ims-database-server/databases/mgl
    :param start_datetime: request start time (if not localized, UTC assumed)
    :type start_datetime: datetime.datetime
    :param end_datetime: request end time (if not localized, UTC assumed)
    :type end_datetime: datetime.datetime
    :param site_ids: list of sites for which data should be read
    :type site_ids: list or integer
    :param format: Requested data format ('possible values: binary and binary-gz')
    :type format: str
    :param network: Network name (default = '')
    :type network: str
    :param dtype: output type for mseed
    :return: microquake.core.stream.Stream
    """

    """
    binary file structure:
    * a binary header of size N bytes, consisting of
        - header size written as int32
        - netid written as int32
        - siteid written as int32
        - start time written as int64(time in nanoseconds)
        - end time written as int64(time in nanoseconds)
        - netADC id written as int32
        - sensor id written as int32
        - attenuator id written as int32
        - attenuator configuration id written as int32
        - remainder of bytes(N minus total so far) written as zero
        padded.
    * A sequence of 20 - byte samples, each consisting of
        - sample timestamp, written as int64(time in nanoseconds)
        - raw X value as float32
        - raw Y value as float32
        - raw Z value as float32
    """

    import calendar
    from datetime import datetime
    from gzip import GzipFile
    import struct
    import numpy as np
    from microquake.core import Trace, Stats, Stream, UTCDateTime
    import sys

    if sys.version_info[0] < 3:
        from StringIO import StringIO
    else:
        from io import StringIO, BytesIO

    if isinstance(site_ids, int):
        site_ids = [site_ids]

    start_datetime_utc = UTCDateTime(start_datetime)
    end_datetime_utc = UTCDateTime(end_datetime)

    time_start = calendar.timegm(start_datetime_utc.timetuple()) * 1e9 + start_datetime_utc.microsecond * 1e3 - 1e9
    time_end = calendar.timegm(end_datetime_utc.timetuple()) * 1e9 + end_datetime_utc.microsecond * 1e3 + 1e9
    url_cont = base_url + '/continuous-seismogram?' + \
               'startTimeNanos=%d&endTimeNanos=%d&siteId' + \
               '=%d&format=%s'

    traces = []
    for site in site_ids:

        r = open(base_url, 'rb')

        if format == 'binary-gz':
            fileobj = GzipFile(fileobj=BytesIO(r.content))
        elif format == 'binary':
            fileobj = r
        else:
            raise Exception('unsuported format!')
            return

        fileobj.seek(0)

        # Reading header
        try:
            header_size = struct.unpack('>i', fileobj.read(4))[0]
            net_id = struct.unpack('>i', fileobj.read(4))[0]
            site_id = struct.unpack('>i', fileobj.read(4))[0]
            starttime = struct.unpack('>q', fileobj.read(8))[0]
            endtime = struct.unpack('>q', fileobj.read(8))[0]
            netADC_id = struct.unpack('>i', fileobj.read(4))[0]
            sensor_id = struct.unpack('>i', fileobj.read(4))[0]
            attenuator_id = struct.unpack('>i', fileobj.read(4))[0]
            attenuator_config_id = struct.unpack('>i', fileobj.read(4))[0]

            # Reading data
            fileobj.seek(header_size)
            content = fileobj.read()

            npts = int(len(content) / 20)

            time = np.zeros(npts)
            X = np.zeros(npts)
            Y = np.zeros(npts)
            Z = np.zeros(npts)
            for i in range(0, npts):
                s = 20 * i
                time[i] = (struct.unpack('>q', content[s:s + 8])[0])
                X[i] = (struct.unpack('>f', content[s + 8:s + 12])[0])
                Y[i] = (struct.unpack('>f', content[s + 12:s + 16])[0])
                Z[i] = (struct.unpack('>f', content[s + 16:s + 20])[0])

            # Tracer()()

            sampling_rate = int(np.round(1 / np.mean(np.diff(time) * 1e-9)))

            t_int = np.int64(np.arange(0, len(X))) / \
                    np.float(sampling_rate) * 1e9 + time[0]

            # if not np.all(np.isnan(X)):
            #     X_int = np.interp(time, t_int, X)
            #     Y_int = np.interp(time, t_int, Y)
            #
            # Z_int = np.interp(time, t_int, Z)

            stats = Stats()
            stats.sampling_rate = sampling_rate
            stats.network = str(network)
            stats.station = str(site)
            time_zone = start_datetime.tzinfo
            stats.starttime = UTCDateTime(datetime.fromtimestamp(starttime * 1.e-9).replace(tzinfo=time_zone))
            microsecond = int(((starttime * 1e-9) - np.floor(starttime * 1e-9)) * 1e6)
            stats.starttime.microsecond = microsecond
            stats.npts = npts

            if not np.all(np.isnan(X)):
                stats.channel = 'X'
                traces.append(Trace(data=X, header=stats))

                stats.channel = 'Y'
                traces.append(Trace(data=Y, header=stats))

            stats.channel = 'Z'
            traces.append(Trace(data=Z, header=stats))

        except:
            print('Unable to read the data stream for sensor %i!\nCheck request start and end times.' %site)

    return Stream(traces=traces).detrend('linear').detrend('demean').trim(starttime=start_datetime_utc,
                                                                          endtime=end_datetime_utc)



@curry
def get_data(base_url, starttime, endtime, overlap, window_length, filter,
             taper, site_id):
    """

    :param base_url: base URL pointing to the seismic sytsem
    :param starttime: start time of the data request window
    :type starttime: datetime.datetime including time zone information, if no
                     time zone information provided UTC is assumed
    :param endtime: end time of the data request window
    :type endtime: datetime.datetime same as starttime
    :param overlap: size of the overlapping windo
    :param window_length: length of window
    :param filter: dictionary with parameter for the filter
    :param taper: dictionary containing the parameter for the taper
    :param site_id: id of the size
    :return:
    """
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
    dts = range(0, nsec, window_length)
    dtimes = [timedelta(seconds=dt) for dt in dts]

    starttime = starttime
    endtime = starttime + timedelta(seconds=len(dts) * window_length) + overlap
    try:
        # Use below for Global Use
        # st = web_api.get_continuous(base_url, starttime - overlap, endtime + overlap, site_ids=[site_id])

        # Use below for local use
        #base_url = '/Users/hanee/Rio_Tinto/sample_data/20170408_224235.mseed'
        st = get_continuous_local(base_url, starttime - overlap, endtime + overlap,
                                          site_ids=[site_id], format='binary')

    except Exception as e:
        print(e)
        return

    if not st:
        return

    sts = []

    for dtime in dtimes:
        stime = starttime + dtime - overlap
        etime = starttime + dtime + timedelta(seconds=window_length) + overlap
        st_trim = st.copy().trim(starttime=UTCDateTime(stime),
                                 endtime=UTCDateTime(etime))
        st_trim = st_trim.taper(1, **taper)
        st_trim = st_trim.filter(**filter)
        sts.append((stime.strftime('%Y%m%d%H%M%S%f'), st_trim))

    return sts


def partition(mapped_values):
    import collections

    partitionned_data = collections.defaultdict(list)
    for ele in mapped_values:
        if not ele:
            continue
        for key, value in ele:
            partitionned_data[key].append(value)

    return partitionned_data.values()


def reduce(partitionned_data):
    from microquake.core.stream import Stream

    datas = partitionned_data

    traces = []
    for data in datas:
        for tr in data:
            traces.append(tr)

    return Stream(traces=traces)


def request_handler():

    config_dir = os.environ['SPP_CONFIG']
    common_dir = os.environ['SPP_COMMON']
    fname = os.path.join(config_dir, 'ims_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['ims_connector']

    if params['data_source']['type'] == 'remote':
        base_url = params['data_source']['location']
    else:
        base_url = params['data_source']['local_location']

    minimum_offset = params["minimum_time_offset"]
    window_length = params["window_length"]
    overlap = timedelta(seconds=params['overlap'])
    period = params['period']
    filter = params['filter']
    taper = params['taper']
    max_window_length = params['max_window_length']

    workers = params['multiprocessing']['workers']

    time_zone = time.get_time_zone()

    # get end time of last window, note that time in the file should be in
    # local time
    ftime = os.path.join(common_dir, 'ingest_info.txt')

    starttime = datetime(2018, 5, 23, 18, 49, 0, tzinfo=time_zone)

    endtime_ = datetime(2018, 5, 23, 19, 40, 0, tzinfo=time_zone)

    while starttime < endtime_:

        now = datetime.now().replace(tzinfo=time_zone)

        if not os.path.isfile(ftime):
            starttime = now \
                        - timedelta(seconds=minimum_offset) \
                        - timedelta(seconds=period)

            endtime = now - timedelta(seconds=minimum_offset)

        else:
            with open(ftime, 'r') as timefile:
                starttime = parser.parse(timefile.readline()) - overlap
                starttime = starttime.replace(tzinfo=time_zone)

                dt = (now - starttime).total_seconds()
                if dt - minimum_offset > max_window_length:
                    starttime = now \
                                - timedelta(seconds=(minimum_offset +
                                                         max_window_length)) - \
                                overlap

        endtime = now - timedelta(seconds=minimum_offset)

        endtime = starttime + timedelta(seconds=period)

        station_file = os.path.join(common_dir, 'sensors.csv')
        site = read_stations(station_file, has_header=True)

        st_code = [int(station.code) for station in site.stations()]

        p = Pool(workers)
        start = timer()
        map_responses = p.map(get_data(base_url, starttime, endtime, overlap,
                                       window_length, filter, taper), st_code)

        partitionned = partition(map_responses)

        data_blocks = []
        from spp.time import localize

        for group in partitionned:
            block = reduce(group)
            stime = localize(block[0].stats.starttime).strftime(
                "%Y%m%d_%H%M%S_%f")
            etime = localize(block[0].stats.endtime).strftime(
                "_%Y%m%d_%H%M%S_%f.mseed")

            fname = stime + etime

            # eventually to be written to Kafka
            #block.write(fname, format='MSEED')
            with open('tmp.txt', 'a') as tmp:
                 tmp.write(fname)
        # blocks = map(reduce, partitionned)

        starttime = endtime

        end = timer()

        print(end - start)

        with open(ftime, 'w') as timefile:
            timefile.write(endtime.strftime("%Y-%m-%d %H:%M:%S.%f"))










