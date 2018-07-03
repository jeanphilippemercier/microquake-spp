from microquake.core import read
from microquake.db.mongo.mongo import MongoDBHandler
import numpy as np
from datetime import datetime
import time
from IPython.core.debugger import Tracer
from multiprocessing import Pool

def encode_stream(stream):
    """
    Encode a stream object for storage in the database
    :param stream: a microquake.core.Trace object to be encoded
    :return: encoded stream in bson format
    """

    from io import BytesIO
    from microquake.core.stream import Stream
    from microquake.core import UTCDateTime
    from microquake.core.util import serializer

    traces = []
    for tr in stream:
        trout = dict()
        trout['stats'] = dict()
        for key in tr.stats.keys():
            if isinstance(tr.stats[key], UTCDateTime):
                trout['stats'][key] = tr.stats[key].datetime
            else:
                trout['stats'][key] = tr.stats[key]

        stout = Stream(traces=[tr])
        buf = BytesIO()
        stout.write(buf, format='MSEED')
        trout['encoded_mseed'] = serializer.encode_base64(buf)
        traces.append(trout)

    return traces


def decode_stream(encoded_stream):
    """
    Decode a stream object encoded in bson
    :param encoded_stream: encoded stream produced by the encode_stream function
    :return: a microquake.core.Stream object
    """

    from microquake.core.stream import Stream
    from microquake.core import read
    from io import BytesIO
    from microquake.core.util import serializer

    traces = []
    for encoded_tr in encoded_stream:
        bstream = serializer.decode_base64(encoded_tr['encoded_mseed'])
        tr = read(BytesIO(bstream))[0]
        traces.append(tr)

    return Stream(traces=traces)


def insert_continuous_mongo(fname, uri='mongodb://localhost:27017/',
                            db_name='test_continuous'):
    """

    :param st: stream
    :param uri: db uri
    :param db_name: db name
    :return:
    """
    st = read(fname)

    from microquake.core import Stream
    db = MongoDBHandler(uri=uri, db_name=db_name)

    documents = []
    for tr in st:
        tr.stats.mseed.encoding = 'FLOAT32'
        tr.data = tr.data.astype(np.float32)
        st_out = Stream(traces=[tr])

        data = encode_stream(st_out)

        starttime_ns = int(np.float64(tr.stats.starttime.timestamp) * 1e9)
        endtime_ns = int(np.float64(tr.stats.endtime.timestamp) * 1e9)
        network = tr.stats.network
        station = tr.stats.station
        channel = tr.stats.channel
        document = {'time_created' : datetime.utcnow(),
                    'network': network,
                    'station': station,
                    'channel': channel,
                    'data': data,
                    'starttime': starttime_ns,
                    'endtime': endtime_ns}
        db.db['waveforms'].insert_one(document)

    db.disconnect()


def get_time_channel(starttime, endtime, network=None, station=None,
                     channel=None, uri='mongodb://localhost:27017/',
                     db_name='test_continuous'):
    from microquake.core import UTCDateTime, Stream

    db = MongoDBHandler(uri=uri, db_name=db_name)

    starttime_ns = int(np.float64(UTCDateTime(starttime).timestamp) * 1e9)
    endtime_ns = int(np.float64(UTCDateTime(endtime).timestamp) * 1e9)

    if not station:
        query = {'endtime': {'$gte': starttime_ns},
                 'starttime' : {'$lte' : endtime_ns}}

    else:

        query = {'endtime': {'$gte': starttime_ns},
                 'starttime' : {'$lte' : endtime_ns},
                 'station': station}




    # query = {'network': network,
    #          'station': station,
    #          'channel': channel}

    traces = []

    sampling_rate = 6000
    for rec in db.db.waveforms.find(query):
        tr = decode_stream(rec['data'])[0]
        if tr.stats.sampling_rate != sampling_rate:
            continue
        traces.append(tr)

    st = Stream(traces=traces)
    st.merge()

    Tracer()()


    return db.db.waveforms.find(query)



from glob import glob

# files = glob('/Users/jpmercier/continuous_data/*.mseed')
# P = Pool(8)
#
# P.map(insert_continuous_mongo, files)
#
# for fle in glob('/Users/jpmercier/continuous_data/*.mseed'):
#     st_t = time.time()
#     insert_continuous_mongo(fle)
#     ed_t = time.time()
#     print (ed_t - st_t)

from microquake.core import UTCDateTime

starttime = UTCDateTime(2018,5,23,10,49,20)
endtime = UTCDateTime(2018,5,23,10,49,26)

get_time_channel(starttime, endtime)

# starttime_ns = int(np.float64(UTCDateTime(starttime).timestamp) * 1e9)
# endtime_ns = int(np.float64(UTCDateTime(endtime).timestamp) * 1e9)
#
#
# station = '53'
#
# query = {'endtime': {'$gte': starttime_ns},
#          'starttime' : {'$lte' : endtime_ns},
#           'station': station}

