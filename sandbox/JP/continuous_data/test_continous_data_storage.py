from pymongo import MongoClient
from tqdm import tqdm
from time import time
from struct import unpack
from microquake.io.waveform import mseed_date_from_header
from microquake.core import read
from io import BytesIO
import numpy as np
import redis
import uuid
import base64


def write_mseed_to_mongo(mongo_collection, file_name, mseed_reclen=4096):
    with open(file_name, 'rb') as mseed:
        mseed_bytes = mseed.read()
        starts = np.arange(0, len(mseed_bytes), mseed_reclen)
        records = []

        for k, start in enumerate(tqdm(starts)):
            end = start + mseed_reclen
            record = mseed_bytes[start:end]

            station = unpack('5s', record[8:13])[0].strip().decode()
            channel = unpack('3s', record[15:18])[0].strip().decode()
            network = unpack('2s', record[18:20])[0].strip().decode()
            number_of_sample = unpack('>H', record[30:32])[0]
            sample_rate_factor = unpack('>h', record[32:34])[0]
            sample_rate_multiplier = unpack('>h', record[34:36])[0]
            sampling_rate = sample_rate_factor / sample_rate_multiplier

            dt = np.dtype(np.float32)
            dt = dt.newbyteorder('>')
            data = np.trim_zeros(np.frombuffer(record[-number_of_sample * 4:],
                                               dtype=dt),'b')

            # remove zero at the end
            number_of_sample = len(data)
            starttime = mseed_date_from_header(record)
            endtime = starttime + (number_of_sample - 1) / float(sampling_rate)

            starttime = int(starttime.datetime.timestamp() * 1e9)
            endtime = (int(endtime.datetime.timestamp() * 1e9))

            seismic = {'station': station, 'channel': channel, 'network':
                       network, 'starttime': starttime, 'endtime': endtime,
                       'data':data.tolist()}

            records.append(seismic)

            mongo_collection.insert_one(seismic)

def read_mseed_from_mongo(mongo_collection, starttime, endtime, station=None,
                          channel=None,network=None):

    starttime = starttime.datetime.timestamp()
    endtime = endtime.datetime.timestamp()
    result = mongo_collection.find({'starttime':{'$gte': starttime},
                                    'endtime':{'$lte':endtime},
                                    'station':station, 'channel':channel,
                                    'network':network})





# r = redis.Redis()
# with open(file_name, 'rb') as mseed:
#     mseed_bytes = mseed.read()
#     r.set(str(uuid.uuid(4)), mseed_bytes)



client = MongoClient('localhost', 27017)
mongodb = client["seismic_test"]
mongo_collection = mongodb["continuous"]
file_name = '2018-11-08T10_21_49.898496Z.mseed'
t0 = time()
write_mseed_to_mongo(mongo_collection, file_name)
t1 = time()


t2 = time()
r = redis.Redis()
key = str(uuid.uuid4())
with open(file_name, 'rb') as mseed:
    mseed_bytes = mseed.read()
    r.set(key, mseed_bytes, ex=600)

bytes = r.get(key)
st_io=BytesIO(bytes)
st = read(st_io, format='MSEED')
t3 = time()

t4 = time()
st = read(file_name)
# st_io = BytesIO()
# st.write(st_io, format='MSEED')
# r.set(str(uuid.uuid4(), st_io.getvalue() , ex=600))
t5 = time()

print('Wrote into mongo in %0.3f' % (t1 - t0))
print('wrote bytes into redis in %0.3f' % (t3 - t2))
print('wrote stream into redis in %0.3f' % (t5 - t4))


# if records:
#     mongo_collection.insert_many(records)

        # input('bubu')

        # mseed_date_from_header(block4096)




# def decompose_mseed(mseed_bytes, mseed_reclen=4096):
#     """
#     Return dict with key as epoch starttime and val
#     as concatenated mseed blocks which share that
#     starttime.
#     """
#     starts = np.arange(0, len(mseed_bytes), mseed_reclen)
#     dchunks = {}
#
#     for start in starts:
#         end = start + mseed_reclen
#         chunk = mseed_bytes[start:end]
#         dt = mseed_date_from_header(chunk)
#         key = int(datetime_to_epoch_sec(dt) * 1000)
#
#         if key not in dchunks:
#             dchunks[key] = b''
#         dchunks[key] += chunk
#
#     return dchunks