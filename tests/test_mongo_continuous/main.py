from spp.data_connector import core
from importlib import reload
from time import time

reload(core)
for k in range(0, 10):
    st = time()
    core.load_data()
    et = time()
    print(et-st)

# rh = core.requestHandler()
# rh.load_data()


# from microquake.core import read
# from microquake.db.mongo.mongo import MongoDBHandler
# import numpy as np
# from datetime import datetime
# import time
# from IPython.core.debugger import Tracer
# from multiprocessing import Pool
# from microquake.core import Trace
# from io import BytesIO
# import pickle
# from microquake.IMS import web_api
# from spp.time import get_time_zone
# from spp.utils import get_stations
# from datetime import timedelta
#
#
# def insert_continuous_mongo(st, uri='mongodb://localhost:27017/',
#                             db_name='test_continuous'):
#     """
#
#     :param st: stream
#     :param uri: db uri
#     :param db_name: db name
#     :return:
#     """
#     #st = read(fname)
#
#     from microquake.core import Stream
#     from microquake.core.util import serializer
#     db = MongoDBHandler(uri=uri, db_name=db_name)
#     from base64 import b64encode
#
#     documents = []
#     for tr in st:
#         try:
#             tr.stats.mseed.encoding = 'FLOAT32'
#         except:
#             pass
#         tr.data = tr.data.astype(np.float32)
#         st_out = Stream(traces=[tr])
#
#         data = tr.data.astype(np.float32)
#
#         stats_b64 = b64encode(pickle.dumps(tr.stats))
#
#         starttime_ns = int(np.float64(tr.stats.starttime.timestamp) * 1e9)
#         endtime_ns = int(np.float64(tr.stats.endtime.timestamp) * 1e9)
#         network = tr.stats.network
#         station = tr.stats.station
#         channel = tr.stats.channel
#         document = {'time_created' : datetime.utcnow(),
#                     'network': network,
#                     'station': station,
#                     'channel': channel,
#                     'data': data.tolist(),
#                     'stats': stats_b64,
#                     'starttime': starttime_ns,
#                     'endtime': endtime_ns}
#         db.db['waveforms'].insert_one(document)
#
#     db.disconnect()
#
#
# def get_time_channel(starttime, endtime, network=None, station=None,
#                      channel=None, uri='mongodb://localhost:27017/',
#                      db_name='test_continuous'):
#     from microquake.core import UTCDateTime, Stream
#     from microquake.core.util import serializer
#     import numpy as np
#     from base64 import b64decode
#
#     db = MongoDBHandler(uri=uri, db_name=db_name)
#
#     starttime_ns = int(np.float64(UTCDateTime(starttime).timestamp) * 1e9)
#     endtime_ns = int(np.float64(UTCDateTime(endtime).timestamp) * 1e9)
#
#     if not station:
#         query = {'endtime': {'$gte': starttime_ns},
#                  'starttime' : {'$lte' : endtime_ns}}
#
#     else:
#
#         query = {'endtime': {'$gte': starttime_ns},
#                  'starttime' : {'$lte' : endtime_ns},
#                  'station': station}
#
#
#     sampling_rate = 6000
#     traces = []
#     for rec in db.db.waveforms.find(query):
#         stats = pickle.loads(b64decode(rec['stats']))
#
#         tr = Trace(data=np.array(rec['data']), header=stats)
#         if tr.stats.sampling_rate != sampling_rate:
#             continue
#         traces.append(tr)
#
#     st = Stream(traces=traces)
#     st.merge()
#
#     Tracer()()
#
#
#     return db.db.waveforms.find(query)
#
#
# def write_files(offset):
#
#     base_url = 'http://10.95.64.12:8002/ims-database-server/databases/mgl'
#
#     tz = get_time_zone()
#
#     starttime = datetime(2018, 7, 6, 17, 40, 20, tzinfo=tz)
#     site = get_stations()
#
#     record_length = 0.1 # record length in second
#     site_ids = [int(station.code) for station in site.stations()]
#
#     ts = starttime + timedelta(seconds=offset)
#     # print(packet, ts)
#     te = ts + timedelta(seconds=offset)
#     st = web_api.get_continuous(base_url, ts, te, site_ids)
#     filename = '/Users/jpmercier/data_100ms/%s_%s.mseed' \
#                % (ts.strftime('%Y%m%d%H%M%S%f'),
#                   te.strftime('%Y%m%d%H%M%S%f'))
#     st.write(filename, format='MSEED')
#     insert_continuous_mongo(st)
#
#
#
# from microquake.IMS import web_api
# from spp.time import get_time_zone
# from spp.utils import get_stations
# from microquake.core import UTCDateTime
# from datetime import timedelta
# from multiprocessing import Pool
# base_url = 'http://10.95.64.12:8002/ims-database-server/databases/mgl'
#
# # tz = get_time_zone()
# #
# # starttime = datetime(2018, 7, 6, 17, 40, 20, tzinfo=tz)
# # site = get_stations()
# #
# record_length = 120 # record length in second
# # site_ids = [int(station.code) for station in site.stations()]
#
# # p = Pool(8)
#
# delays = np.arange(0,1000000) * record_length
# # p.map(write_files, delays)
#
# for delay in delays:
#     write_files(delay)
#
# # for packet in range(0,1000000):
#     # ts = starttime + packet * timedelta(seconds=record_length)
#     # print(packet, ts)
#     # te = ts + timedelta(seconds=record_length)
#     # st = web_api.get_continuous(base_url, ts, te, site_ids)
#     # filename = '/Users/jpmercier/data_100ms/%s_%s.mseed' \
#     #            % (ts.strftime('%Y%m%d%H%M%S%f'),
#     #               te.strftime('%Y%m%d%H%M%S%f'))
#     # st.write(filename)
#
#
#
#
# # web_api.get_continuous(base_url=base_url, )
#
# from glob import glob
#
# # files = glob('/Users/jpmercier/continuous_data/*.mseed')
# # P = Pool(8)
# # P.map(insert_continuous_mongo, files)
#
# # for fle in glob('/Users/jpmercier/continuous_data/*.mseed'):
# #     st_t = time.time()
# #     insert_continuous_mongo(fle)
# #     ed_t = time.time()
# #     print (ed_t - st_t)
#
#
# # from microquake.core import UTCDateTime
# #
# # starttime = UTCDateTime(2018,5,23,10,49,20)
# # endtime = UTCDateTime(2018,5,23,10,49,26)
# #
# # get_time_channel(starttime, endtime)
#
# # starttime_ns = int(np.float64(UTCDateTime(starttime).timestamp) * 1e9)
# # endtime_ns = int(np.float64(UTCDateTime(endtime).timestamp) * 1e9)
# #
# #
# # station = '53'
# #
# # query = {'endtime': {'$gte': starttime_ns},
# #          'starttime' : {'$lte' : endtime_ns},
# #           'station': station}
#
