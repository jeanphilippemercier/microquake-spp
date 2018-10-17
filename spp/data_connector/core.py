from microquake.core import read_stations
from datetime import datetime, timedelta
from spp.time import get_time_zone
import os
from pathos.multiprocessing import Pool
from toolz.functoolz import curry
from dateutil import parser
import numpy as np
import time
from microquake.db.mongo.mongo import MongoDBHandler
from glob import glob
from spp.utils.config import Configuration
from spp.utils import log_handler


logger = log_handler.get_logger("Data Connector", 'data_connector.log')
CONFIG = Configuration()


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

def write_mseed_chunk_to_mongo(mseed_byte_array):
    """
    :param mseed_byte_array:
    :return:
    """

    from io import BytesIO
    from microquake.core import read

    stime = time.time()

    mongo_conn = MongoDBHandler(uri=CONFIG.DB['uri'], db_name=CONFIG.DB['db_name'])
    collection_name = CONFIG.DB['traces_collection']

    mseed_chunk_size = 4096

    starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)

    for start in starts:
        end = start + mseed_chunk_size
        chunk = mseed_byte_array[start:end]
        st = read(BytesIO(chunk), format='MSEED')

        traces_list = st.to_traces_json()
        mongo_conn.insert_many(collection_name, traces_list)
    mongo_conn.disconnect()
    etime = time.time() - stime
    logger.info("Inserted stream chunks into MongoDB in: %.2f" % etime)


def write_mseed_chunk_to_kafka(mseed_byte_array):
    """

    :param mseed_byte_array:
    :return:
    """
    from pandas import DataFrame
    from struct import unpack, pack
    from spp.utils.kafka import KafkaHandler
    from datetime import datetime

    stime = time.time()

    kafka_brokers = CONFIG.DATA_CONNECTOR['kafka']['brokers']
    kafka_topic = CONFIG.DATA_CONNECTOR['kafka']['topic']
    kafka_handler = KafkaHandler(kafka_brokers)
    mseed_chunk_size = 4096
    keys = []
    blobs = []

    starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)

    for start in starts:
        end = start + mseed_chunk_size
        chunk = mseed_byte_array[start:end]

        y = unpack('>H',chunk[20:22])[0]
        DoY = unpack('>H', chunk[22:24])[0]
        H = unpack('>B', chunk[24:25])[0]
        M = unpack('>B', chunk[25:26])[0]
        S = unpack('>B', chunk[26:27])[0]
        r = unpack('>B', chunk[27:28])[0]
        ToMS = unpack('>H', chunk[28:30])[0]

        dt = datetime.strptime('%s/%0.3d %0.2d:%0.2d:%0.2d.%0.3d'
                               % (y, DoY, H, M, S, ToMS),
                               '%Y/%j %H:%M:%S.%f')
        keys.append(dt)
        blobs.append(chunk)

    df = DataFrame({'key': keys, 'blob': blobs})

    df_grouped = df.groupby(['key'])

    logger.debug("Grouped DF Stats:" + str(df_grouped.size()))

    for name, group in df_grouped:
        data = b''
        for g in group['blob'].values:
            data += g
        timestamp = int(name.timestamp() * 1e3)
        key = name.strftime('%Y-%d-%m %H:%M:%S.%f').encode('utf-8')
        kafka_handler.send_to_kafka(kafka_topic, message=data, key=key,
                                    timestamp=int(timestamp))
    kafka_handler.producer.flush()
    etime = time.time() - stime
    logger.info("==> Inserted stream chunks into Kafka in: %.2f" % etime)


def read_realtime_info():
    pass


def write_realtime_info():
    pass


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
    from microquake.core import UTCDateTime
    from microquake.IMS import web_api
    from datetime import timedelta
    from importlib import reload
    from spp.utils import get_stations

    reload(web_api)

    params = CONFIG.DATA_CONNECTOR

    site = get_stations()

    period = endtime - starttime
    nsec = period.total_seconds()
    nperiod = int(np.floor(nsec / window_length))
    dts = np.linspace(0, nsec, nperiod)
    # dts = range(0, nsec, window_length)
    dtimes = [timedelta(seconds=dt) for dt in dts]

    starttime = starttime
    endtime = starttime + timedelta(seconds=len(dts) * window_length) + overlap

    # try:
    # Use below for Global Use
    st = web_api.get_continuous(base_url, starttime - overlap,
                                endtime + overlap, site_ids=site_id)


    if not st:
        return

    for k, tr in enumerate(st):
        station = site.select(station=tr.stats.station).stations()[0]
        st[k].stats.location = station.long_name
        st[k].stats.network = site.networks[0].code

    period = st[0].stats.endtime - st[0].stats.starttime
    nperiod = int(np.floor(period / window_length))
    dts = np.linspace(0, nperiod * window_length, nperiod)
    # dts = range(0, nsec, window_length)
    dtimes = [timedelta(seconds=dt) for dt in dts]

    # except Exception as e:
    #     print(e)
    #     return

    sts = []

    for dtime in dtimes:
        buffer = timedelta(seconds = params['taper']['max_length'])
        stime = starttime + dtime - buffer
        etime = starttime + dtime + timedelta(seconds=window_length) + \
                overlap + buffer
        st_trim = st.copy().trim(starttime=UTCDateTime(stime),
                                 endtime=UTCDateTime(etime))

        nan_flag = False
        for k, tr in enumerate(st_trim):
            if np.isnan(np.mean(tr.data)):
                nan_flag = True
            st_trim[k].data.astype(np.float32)

        if nan_flag:
            continue

        st_trim = st_trim.detrend('demean')
        st_trim = st_trim.detrend('linear')

        st_trim = st_trim.taper(1, **taper)
        st_trim = st_trim.filter(**filter)

        stime = stime + buffer
        etime = etime - buffer

        st_trim = st_trim.trim(starttime=UTCDateTime(stime),
                               endtime=UTCDateTime(etime))
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

    common_dir = os.environ['SPP_COMMON']

    params = CONFIG.DATA_CONNECTOR

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
    playback = params['playback']

    workers = params['multiprocessing']['workers']

    time_zone = get_time_zone()

    # get end time of last window, note that time in the file should be in
    # local time
    ftime = os.path.join(common_dir, 'ingest_info.txt')

    now = datetime.now().replace(tzinfo=time_zone)

    if not os.path.isfile(ftime):
        starttime = now \
                    - timedelta(seconds=minimum_offset) \
                    - timedelta(seconds=max_window_length)

    else:
        with open(ftime, 'r') as timefile:
            starttime = parser.parse(timefile.readline()) - overlap
            starttime = starttime.replace(tzinfo=time_zone)

            dt = (now - starttime).total_seconds()
            if dt - minimum_offset > max_window_length:
                starttime = now - timedelta(seconds=(minimum_offset +
                                                     max_window_length)) - \
                            overlap

    endtime = starttime + timedelta(seconds=max_window_length)

    if endtime > now - timedelta(seconds=minimum_offset):
        endtime = now - timedelta(seconds=minimum_offset) + overlap

    station_file = os.path.join(common_dir, 'sensors.csv')
    site = read_stations(station_file, has_header=True)

    st_codes = np.array([int(station.code) for station in site.stations()])

    st_codes = st_codes[20:21].tolist()

    # if workers > 1:
    p = Pool(workers)

    map_responses = map(get_data(base_url, starttime, endtime, overlap,
                                    window_length, filter, taper), st_codes)

    partitionned = partition(map_responses)

    from spp.time import localize

    with open(ftime, 'w') as timefile:
        timefile.write(endtime.strftime("%Y-%m-%d %H:%M:%S.%f"))

    for group in partitionned:
        block = reduce(group)
        if len(block) == 0:
            continue
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


def write_to_local(stream_object, location):
    """

    :param path:
    :return:
    """

    import os

    fname = stream_object[0].stats.starttime.strftime("%Y%m%d%H%M%S_%f_") + \
            stream_object[0].stats.endtime.strftime("%Y%m%d%H%M%S_%f.mseed")
    path = os.path.join(location, fname)
    stream_object.write(path, format='MSEED')


def write_to_kafka(stream_object, brokers, kafka_topic):
    """

    :param stream_object: a microquake.stream.Stream object containing the
    waveforms
    :param brokers: a ',' delimited string containing the information on the
    kafka brokers (e.g., "kafka-node-001:9092,kafka-node-002:9092,
    kafka-node-003:9092")
    :param kafka_topic:
    :return:
    """
    from spp.utils.kafka import KafkaHandler
    from io import BytesIO
    import sys

    kafka_handler_obj = KafkaHandler(brokers)
    s_time = time.time()
    buf = BytesIO()
    stream_object.write(buf, format='MSEED')
    kafka_msg = buf.getvalue()  # serializer.encode_base64(buf)
    msg_key = str(stream_object[0].stats.starttime)
    end_time_preparation = time.time() - s_time

    msg_size = (sys.getsizeof(kafka_msg) / 1024 / 1024)

    s_time = time.time()
    kafka_handler_obj.send_to_kafka(kafka_topic, kafka_msg,
                                    msg_key.encode('utf-8'))

    kafka_handler_obj.producer.flush()

    end_time_submission = time.time() - s_time

    logger.info("Object Size:", "%.2f" % msg_size, "MB",
                "Key:", msg_key,
                ", Preparation took:", "%.2f" % end_time_preparation,
                ", Submission took:", "%.2f" % end_time_submission)


def convert_stream_to_bytes(stream_object):
    from io import BytesIO

    output = BytesIO()
    stream_object.write(output, format="MSEED")
    return output.getvalue()


def write_data(stream_object):
    """
    :param destination: destination to write file (e.g., local (filesystem),
    kafka, MongoDB)
    :param kwargs: arguments to
    :return:
    """

    params = CONFIG.DATA_CONNECTOR

    destination = params['data_destination']['type'].lower()

    # Temp Solution to convert stream object into bytes
    # will be enhanced in future
    stream_bytes = convert_stream_to_bytes(stream_object)

    if "kafka" in destination:
        write_mseed_chunk_to_kafka(stream_bytes)
    if 'mongo' in destination:
        write_mseed_chunk_to_mongo(stream_bytes)


    # if 'kafka' in destination:
    #     brokers = params['kafka']['brokers']
    #     kafka_topic = params['kafka']['kafka_topic']
    #     write_to_kafka(stream_object, brokers, kafka_topic)




def load_data():

    fname = 'load_data'
    params = CONFIG.DATA_CONNECTOR

    if params['data_source']['type'] == 'remote':
        for st in request_handler():
            # print(st)
            write_data(st)
            # write to Kafka
            # write_to_kafka(kafka, kafka_topic, st)

    elif params['data_source']['type'] == 'local':
        location = params['data_source']['location']
        if os.path.isfile(location):
            print("==> Processing single file %s" % location)
            st = get_continuous_local(location, file_name=location)
            write_data(st)
            return
        period = params['period']
        window_length = params['window_length']
        start_time_full = time.time()

        use_glob_pattern = False
        if 'use_glob_pattern' in params['data_source']:
            use_glob_pattern = params['data_source']['use_glob_pattern']

        if use_glob_pattern:
            file_list = glob(location)
            for f in file_list:
                logger.info('%s.%s: Inject playback file:%s' % (__name__, fname, f))
                st = get_continuous_local(f, file_name=f)
                write_data(st)
                #exit()
            #exit()


        # simulator that returns random files
        # '''
        # for i in np.arange(0, period, window_length):
        #     print("==> Processing (", i, " from", period, ")")
        #     start_time_load = time.time()
        #     st = request_handler_local(location)
        #     end_time_load = time.time() - start_time_load
        #     print("==> Fetching File took: ", "%.2f" % end_time_load)
        #     write_data(st)
        # '''



        # print("==> Flushing and Closing Kafka....")
        # start_time_flush = time.time()
        # #kafka.producer.flush()
        # end_time_flush = time.time() - start_time_flush
        # print("==> Flushing Kafka took: ", "%.2f" % end_time_flush)
        #
        # end_time_full = time.time() - start_time_full
        # print("==> Total Time Taken: ", "%.2f" % end_time_full)
        #
        # print("==> Program Exit")










