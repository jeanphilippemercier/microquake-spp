#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is the data connector to run the data connector

"""

import ray
try:
    ray.init()
except:
    pass
from spp.utils.application import Application
from obspy.core import UTCDateTime
from confluent_kafka import Producer
from struct import unpack
from microquake.io.waveform import mseed_date_from_header
import toml
# from kafka import KafkaProducer

# reading the continuous data connector specific settings (not yet
# integrated to the settings.toml)

app = Application()
kproducer = Producer(
            {"bootstrap.servers": app.settings.get('kafka').brokers}
            )
p = Producer({'bootstrap.servers': 'broker:9092'})

base_url = app.settings.get('data_connector').path
inventory = app.get_inventory()
station_codes = [station.code for station in inventory.stations()]

end_time = UTCDateTime.now()
start_time = end_time - 3 * 60

tz = app.get_time_zone()

def extract_mseed_header(mseed_bytes):
    record = mseed_bytes
    station = unpack('5s', record[8:13])[0].strip().decode()
    channel = unpack('3s', record[15:18])[0].strip().decode()
    network = unpack('2s', record[18:20])[0].strip().decode()
    number_of_sample = unpack('>H', record[30:32])[0]
    sample_rate_factor = unpack('>h', record[32:34])[0]
    sample_rate_multiplier = unpack('>h', record[34:36])[0]
    sampling_rate = sample_rate_factor / sample_rate_multiplier

    dt = np.dtype(np.float32)
    dt = dt.newbyteorder('>')
    data = np.trim_zeros(np.frombuffer(
        record[-number_of_sample * 4:], dtype=dt), 'b')

    # remove zero at the end
    number_of_sample = len(data)
    starttime = mseed_date_from_header(record)
    endtime = (starttime + (number_of_sample - 1) / float(sampling_rate))

    trace_dict = {}
    stats = {'station_code': station,
             'channel': channel,
             'network': network,
             'start_time': starttime.datetime,
             'end_time': endtime.datetime,
             'sampling_rate': sampling_rate}

    return stats


def write_mseed_chunk(stream, brokers, mseed_chunk_size=4096):

    from io import BytesIO
    from microquake.core import read
    import numpy as np
    from uuid import uuid4
    from confluent_kafka import Producer
    import time

    p = Producer({'bootstrap.servers': 'broker:9092'})
    # kproducer = Producer(
    #     {"bootstrap.servers": brokers}
    # )

    bio = BytesIO()
    stream.write(bio, format='mseed')

    mseed_byte_array = bio.getvalue()

    starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)

    traces = []
    t0 = time.time()
    for k, start in enumerate(starts):
        p.poll(0)
        end = start + mseed_chunk_size
        chunk = mseed_byte_array[start:end]
        header = extract_mseed_header(chunk)
        timestamp = int(header['start_time'].timestamp() * 1e3)
        p.produce('continuous_data', chunk, header['station_code'],
                  timestamp=timestamp)
    p.flush()
    t1 = time.time()
    print(t1 - t0)

    return 1

from io import BytesIO
from microquake.clients.ims import web_client
from microquake.core import read, UTCDateTime
import numpy as np

brokers = app.settings.get('kafka').brokers


@ray.remote
def extract_data_from_ims(ims_base_url, station_code,
                          start_time, end_time, tz, brokers):

    #time.sleep(1)
    #return 1
    st = web_client.get_continuous(ims_base_url, start_time, end_time,
                                   [station_code], tz)

    write_mseed_chunk(st, brokers)


    return (station_code, st)

results = ray.get([extract_data_from_ims.remote(base_url, station_code,
                                                start_time, end_time, tz,
                                                brokers)
                   for station_code in station_codes[35:36]])

write_mseed_chunk(results[0][1], brokers, mseed_chunk_size=4096)
#

#
#
# # import argparse
# # import sys
# import logging
# import toml
#
# import yappi
#
# __author__ = "jpmercier"
# __copyright__ = "jpmercier"
# __license__ = "GPL_V3"
# #
# logger = logging.getLogger(__name__)
#
# ### TEST
#
# # connect to the IMS database to get continuous data
# from microquake.IMS import web_client
# from microquake.IMS.web_client import get_continuous
# from spp.utils.application import Application
# from datetime import datetime, timedelta
# import pytz
# import pyspark
# import redis
# from multiprocessing import Pool
# from toolz import curry
# import time
# from pymongo import MongoClient
# from redis import StrictRedis
# from struct import unpack
# from microquake.io.waveform import mseed_date_from_header
# import numpy as np
# from dateutil.parser import parse
# import ray
#
# ray.init()
#
# def init_redis(settings):
#     pass
#
# def extract_mseed_header(mseed_bytes):
#     record = mseed_bytes
#     station = unpack('5s', record[8:13])[0].strip().decode()
#     channel = unpack('3s', record[15:18])[0].strip().decode()
#     network = unpack('2s', record[18:20])[0].strip().decode()
#     number_of_sample = unpack('>H', record[30:32])[0]
#     sample_rate_factor = unpack('>h', record[32:34])[0]
#     sample_rate_multiplier = unpack('>h', record[34:36])[0]
#     sampling_rate = sample_rate_factor / sample_rate_multiplier
#
#     dt = np.dtype(np.float32)
#     dt = dt.newbyteorder('>')
#     data = np.trim_zeros(np.frombuffer(
#         record[-number_of_sample * 4:], dtype=dt), 'b')
#
#     # remove zero at the end
#     number_of_sample = len(data)
#     starttime = mseed_date_from_header(record)
#     endtime = (starttime + (number_of_sample - 1) / float(sampling_rate))
#
#     trace_dict = {}
#     stats = {'station_code': station,
#              'channel': channel,
#              'network': network,
#              'start_time': starttime.datetime,
#              'end_time': endtime.datetime,
#              'sampling_rate': sampling_rate}
#
#     return stats
#
#
# def write_mseed_chunk(stream, redis_settings, mongo_settings,
#                       mseed_chunk_size, db_ttl):
#     """
#
#     Args:
#         stream:
#         redis_settings:
#         mongo_settings:
#         mseed_chunk_size:
#
#     Returns:
#
#     """
#
#     from io import BytesIO
#     from microquake.core import read
#     import numpy as np
#     from uuid import uuid4
#
#     logger.info('connecting to MongoDB')
#     db_name = mongo_settings['collection']['db_name']
#     collection_name = mongo_settings['collection']['collection_name']
#     mongo_client = MongoClient(**mongo_settings['client'])
#     db = mongo_client[db_name]
#     collection = db[collection_name]
#     logger.info('successfully connected to MongoDB')
#
#
#     logger.info('connecting to the Redis database')
#     redis_conn = StrictRedis(**redis_settings)
#     logger.info('successfully connected to the Redis database')
#
#     stime = time.time()
#
#     bio = BytesIO()
#     stream.write(bio, format='mseed')
#
#     mseed_byte_array = bio.getvalue()
#
#     starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)
#     # for tstamp, data in mseed_chunks.items():
#
#     traces = []
#
#     try:
#         collection.create_index("creation_time", expireAfterSeconds=db_ttl)
#     except NameError:
#         collection.drop_index("creation_time_1")
#         collection.create_index("creation_time", expireAfterSeconds=db_ttl)
#
#     for k, start in enumerate(starts):
#         end = start + mseed_chunk_size
#         chunk = mseed_byte_array[start:end]
#         if len(chunk) < mseed_chunk_size:
#             continue
#         st = read(BytesIO(chunk), format='MSEED')
#         if len(st) == 0:
#             continue
#         redis_key = str(uuid4())
#         document = extract_mseed_header(chunk)
#         document['redis_key'] = redis_key
#         document['creation_time'] = datetime.utcnow()
#         document['start_time'] = document['start_time']
#         document['end_time'] = document['end_time']
#         redis_conn.set(redis_key, chunk, ex=db_ttl)
#         result = collection.insert_one(document)
#
#     etime = time.time() - stime
#     logger.info("==> converted stream to chunks in: %.2f" % etime)
#
#     mongo_client.close()
#
#     return 1
#
#
# @ray.remote
# def extract_data_from_ims(ims_base_url, station_code,
#                           start_time, end_time, tz):
#
#     #time.sleep(1)
#     #return 1
#     st = web_client.get_continuous(ims_base_url, start_time, end_time,
#                                     [station_code], tz)
#
#
# def extract_data(module_settings, ims_base_url, tz, redis_settings,
#                  mongo_settings, mseed_chunk_size, db_ttl, station_code):
#     from datetime import datetime
#
#     code = station_code
#     return code
#
#     # logger.info('connecting to MongoDB')
#     # db_name = mongo_settings['collection']['db_name']
#     # collection_name = mongo_settings['collection']['collection_name']
#     # mongo_client = MongoClient(**mongo_settings['client'])
#     # db = mongo_client[db_name]
#     # collection = db[collection_name]
#     # logger.info('successfully connected to MongoDB')
#     #
#     # lst = list(collection.find({'station_code': station_code}).sort([(
#     #     'end_time',-1)]).limit(1))
#     #
#     # # max_time_window_min = module_settings.max_time_window_mins
#     #
#     # end_time = datetime.utcnow()
#     # if lst:
#     #     start_time = lst[0]['end_time']
#     #     # if (end_time - start_time).total_seconds() * 60 > max_time_window_min:
#     #     #     start_time = end_time - timedelta(minutes=max_time_window_min)
#     # else:
#     #     start_time = end_time - timedelta(minutes=2)
#     #
#     #
#     # # from pdb import set_trace; set_trace()
#     #
#     # logger.info('retrieving the continuous data for station %s from %s to '
#     #             '%s' % (code, start_time, end_time))
#     #
#     # print('requesting data for station %s' % code)
#     # end_time = datetime.utcnow()
#     #
#     # print('start time :%s' % start_time)
#     # print('end time   :%s' % end_time)
#     #
#     # # The try is there to handle cases where the response from the IMS
#     # # system is empty. The get_continuous function throws an exception that
#     # # needs to be catched
#     # try:
#     #
#     #
#     # except KeyboardInterrupt:
#     #     exit()
#     # except IndexError:
#     #     return
#     # except Exception:
#     #     return
#     #
#     # return write_mseed_chunk(st, redis_settings, mongo_settings,
#     #                          mseed_chunk_size, db_ttl)
#
#
# # try:
# #     sc = pyspark.SparkContext(master='local[8]')
# # except:
# #     pass
#
# app = Application()
# inventory = app.get_inventory()
# tz = app.get_time_zone()
# base_url = app.settings.data_connector.path
# db_ttl = app.settings.redis_extra.ttl
# mseed_chunk_size = 4096
#
#
# # from pdb import set_trace; set_trace()
#
# # rdd = sc.parallelize([station.code for station in inventory.stations()])
#
# # redis_conn = init_redis(app.settings.redis_db)
#
# # broadcasting the settings
#
# redis_settings = dict(app.settings.redis_db)
# mongo_settings = dict(app.settings.mongo_db)
# ims_base_url = app.settings.data_connector.path
#
# module_settings = app.settings.data_connector
#
# # p = Pool(processes=8)
# f = extract_data(module_settings, ims_base_url, tz, redis_settings,
#                  mongo_settings, mseed_chunk_size, db_ttl)
#
# input('bubu')
#
# sc = pyspark.SparkContext(appName='test', master='spark://master:7077')
#
# stations = [station.code for station in inventory.stations()]
# rdd = sc.parallelize(stations)
#
# f = lambda x: extract_data(module_settings, )
#
# rdd.map(f).collect()
# input('bubu')
#
# start = time.time()
# # p.map(f, [station.code for station in inventory.stations()])
# # f(20)
# end = time.time()
# print(end-start)
#
# # f('20')
#
# # for code in [station.code for station in inventory.stations()]:
# #     f(code)
# # input('aqui')
#
# end_times = {}
#
# yappi.start()
# p.map(f, [station.code for station in inventory.stations()])
#
# func_stats = yappi.get_func_stats()
#
# func_stats.save('callgrind.out.' + datetime.now().isoformat(), 'CALLGRIND')
# yappi.stop()
# yappi.clear_stats()
#
# try:
#     while True:
#         # p.map(f, [station.code for station in inventory.stations()])
#         # f('35')
#         input('gudoe')
#
#         # print(rdd.flatMap(f).collect())
#         # input('alla')
#         # for station in inventory.stations():
#         #     code = station.code
#         #     # if int(code) != 20:
#         #     #     continue
#         #     code = station.code
#         #     print('requesting data for station %s' % code)
#         #     end_time = datetime.now().replace(tzinfo=tz)
#         #     if code in end_times.keys():
#         #         start_time = end_times[code] # the program will need to be
#         #         # stateless
#         #     else:
#         #         start_time = end_time - timedelta(minutes=1)  # 5 minutes
#         #         # before now
#         #
#         #     print('start time :%s' % start_time)
#         #     print('end time   :%s' % end_time)
#         #
#         #     try:
#         #         st = web_client.get_continuous(base_url, start_time, end_time,
#         #                                        [code], tz)
#         #     except KeyboardInterrupt:
#         #         exit()
#         #     except IndexError:
#         #         continue
#         #     except Exception:
#         #         continue
#         #
#         #     if not st:
#         #         continue
#         #
#         #     end_times[code] = st[0].stats.endtime.datetime.replace(
#         #         tzinfo=pytz.utc).astimezone(tz)
#
# except KeyboardInterrupt:
#     pass
#
#
#
#
#
#
#
