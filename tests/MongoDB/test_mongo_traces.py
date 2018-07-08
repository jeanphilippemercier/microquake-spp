from microquake.core import read_events,stream
from microquake.db.mongo.mongo import MongoDBHandler, EventDB, StreamDB
import yaml
import os
import numpy as np
import time
from spp.data_connector import core

# reload(mongo)

# reading the config file


def encode_base64(buffered_object):
    """
    Encode an event for storage in the database
    :param event: a microquake.core.event object
    :return: encoded event object in compressed (bz2) QuakeML format
    """
    from bz2 import compress
    from base64 import b64encode

    return b64encode(buffered_object.getvalue())


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
        trout['encoded_mseed'] = encode_base64(buf)
        traces.append(trout)

    return traces

def convert_stream_to_traces_json(stream):
    """
    Encode a stream object for storage in the database
    :param stream: a microquake.core.Trace object to be encoded
    :return: encoded stream in bson format
    """

    from io import BytesIO
    from microquake.core.stream import Stream,Trace
    from microquake.core import UTCDateTime
    from microquake.core.util import serializer

    traces = []

    for tr in stream:
        trout = dict()
        trout['stats'] = dict()
        for key in tr.stats.keys():
            if isinstance(tr.stats[key], UTCDateTime):
                trout['stats'][key] = int(np.float64(tr.stats[key].timestamp) * 1e9)
            else:
                trout['stats'][key] = tr.stats[key]

        trout['data'] = tr.data.tolist()
        traces.append(trout)

    return traces

config_dir = os.environ['SPP_CONFIG']
config_file = os.path.join(config_dir, 'permanent_db.yaml')

with open(config_file,'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['db']


print("connecting to DB")
mongo_conn = MongoDBHandler(uri=params['uri'], db_name=params['db_name'])
print("inserting into DB")


collection = "traces_json"
location = "/Users/hanee/Rio_Tinto/sample_data/"

## Load mseed files
for i in np.arange(0, 1, 1):
    print("==> Processing (", i, " from", 120, ")")
    start_time_load = time.time()
    st = core.request_handler_local(location)
    #st = core.get_continuous_local(location, "/Users/hanee/Rio_Tinto/sample_data/testing (8).mseed")
    end_time_load = time.time() - start_time_load
    print("==> Fetching File took: ", "%.2f" % end_time_load)

    start_time = time.time()
    traces_list = convert_stream_to_traces_json(st)
    end_time = time.time() - start_time
    print("==> Encoding Stream took: ", "%.2f" % end_time)

    start_time = time.time()
    mongo_conn.db[collection].insert_many(traces_list)
    end_time = time.time() - start_time
    print("==> Saving into Mongo took: ", "%.2f" % end_time_load, "For Records Count:", len(traces_list))
    print(st.__str__(extended=True))

# read waveform
# print("Reading Waveform...")
# json_traces = mongo_conn.db[collection].find({})
# print(json_traces)
#decoded_wf = stream_db.read_full_waveform(event.resource_id.id)
#print(decoded_wf)
print("Closing Connection...")
mongo_conn.disconnect()


