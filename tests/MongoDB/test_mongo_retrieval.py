from microquake.core import read_events,stream
from microquake.db.mongo.mongo import MongoDBHandler, EventDB, StreamDB
import yaml
import os
import time
from microquake.core.trace import Trace
from microquake.core.stream import Stream
from obspy.core.util.attribdict import AttribDict
from obspy.core.utcdatetime import UTCDateTime
import numpy as np
from microquake.core import read

# reload(mongo)

# reading the config file


def get_stream_from_db(st_time, duration):
    start_time = int(np.float64(UTCDateTime(st_time).timestamp) * 1e9)
    end_time = start_time + (int(duration) * 1e9)
    print("Starttime:", start_time)
    filter = {
        'stats.starttime': {
            '$gte': start_time,
            '$lt': end_time
        }
    }

    json_traces = mongo_conn.db[collection].find(filter, {"_id": 0})
    print(json_traces)
    stream = Stream.create_from_json_traces(json_traces)
    return stream


config_dir = os.environ['SPP_CONFIG']
config_file = os.path.join(config_dir, 'permanent_db.yaml')

with open(config_file,'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['db']

print("connecting to DB")
mongo_conn = MongoDBHandler(uri=params['uri'], db_name=params['db_name'])

collection = "traces_json"



print("Reading Waveform 1...")
stream_1 = get_stream_from_db("2018-05-23T11:00:18.000", 1)
#stream_1 = read("/Users/hanee/Rio_Tinto/sample_data/20180523_190018_900000_20180523_190020_100066.mseed", format='MSEED')
print(stream_1.__str__(extended=True))

print("####################")

print("Reading Waveform 2...")
stream_2 = get_stream_from_db("2018-05-23T11:00:19.000", 1)
#stream_2 = read("/Users/hanee/Rio_Tinto/sample_data/20180523_190019_900000_20180523_190021_100066.mseed", format='MSEED')
print(stream_2.__str__(extended=True))

print("####################")

print("After Merging...")
stream_merged = (stream_1 + stream_2).merge(fill_value=0, method=0)

print(stream_merged.__str__(extended=True))
print("Closing Connection...")
mongo_conn.disconnect()


from io import BytesIO
import sys
output = BytesIO()
stream_merged.write(output, format="MSEED")
output_size = sys.getsizeof(output.getvalue())
print(output_size)


