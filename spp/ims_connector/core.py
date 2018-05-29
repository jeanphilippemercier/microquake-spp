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
def get_continuous_local(data_directory, file_name=None):
    """

    :param data_directory: directory where data files are located
    :param file_name: name of a file, if not specified a file is randomly
    selected
    :return: a stream containing the data in the file
    """

    from microquake.core import read

    if file_name:
        return read(file_name, format='MSEED')

    else:
        from glob import glob
        from numpy.random import rand
        from numpy import floor, array
        file_list = array(glob(data_directory + "/*.mseed"))
        index = int(floor(rand() * len(file_list)))
        return read(file_list[index], format='MSEED')


# def get_data_local(local_directory, file_name=None):
#     return get_continuous_local(local_directory, file_name=file_name)


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
        st = web_api.get_continuous(base_url, starttime - overlap, endtime + overlap, site_ids=[site_id])

        # Use below for local use
        #base_url = '/Users/hanee/Rio_Tinto/sample_data/20170408_224235.mseed'
        # st = get_continuous_local()

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

    now = datetime.now().replace(tzinfo=time_zone)

    if not os.path.isfile(ftime):
        starttime = now \
                    - timedelta(seconds=minimum_offset) \
                    - timedelta(seconds=period)

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

    endtime = starttime + timedelta(seconds=period)

    station_file = os.path.join(common_dir, 'sensors.csv')
    site = read_stations(station_file, has_header=True)

    st_code = [int(station.code) for station in site.stations()]

    p = Pool(workers)

    # Tracer()()

    # get_data(base_url, starttime, endtime, overlap,
    #                                window_length, filter, taper).call(51)

    map_responses = p.map(get_data(base_url, starttime, endtime, overlap,
                                   window_length, filter, taper), st_code)

    partitionned = partition(map_responses)

    from spp.time import localize

    with open(ftime, 'w') as timefile:
        timefile.write(endtime.strftime("%Y-%m-%d %H:%M:%S.%f"))

    for group in partitionned:
        block = reduce(group)
        stime = localize(block[0].stats.starttime).strftime(
            "%Y%m%d_%H%M%S_%f")
        etime = localize(block[0].stats.endtime).strftime(
            "_%Y%m%d_%H%M%S_%f.mseed")

        fname = stime + etime

        with open('tmp.txt', 'a') as tmp:
             tmp.write(fname)

        yield block


def request_handler_local(data_directory):
    return get_continuous_local(data_directory)











