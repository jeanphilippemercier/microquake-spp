from microquake.core import read_events,stream
from microquake.db.mongo.mongo import MongoDBHandler, EventDB, StreamDB
import yaml
import os
import numpy as np
import time
from spp.data_connector import core

# reload(mongo)

# reading the config file
config_dir = os.environ['SPP_CONFIG']
config_file = os.path.join(config_dir, 'permanent_db.yaml')

with open(config_file,'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['db']

print("connecting to DB")
mongo_conn = MongoDBHandler(uri=params['uri'], db_name=params['db_name'])
print("inserting into DB")

collection = "traces_json"
location = "/Users/hanee/Rio_Tinto/sample_data"


def convert_stream_to_traces_json(stream):
    traces = []
    for tr in stream:
        trout = tr.to_json()
        traces.append(trout)
    return traces


# Used to load files of dir sequential
from glob import glob
file_list = np.array(glob(location + "/*.mseed"))

## Load mseed files
for i in np.arange(0, len(file_list), 1):
    print("==> Processing (", i, " from", 120, ")")
    print("==> Filename:", file_list[i])
    start_time_load = time.time()
    #st = core.request_handler_local(location)
    # use below line for loading one file only
    st = core.get_continuous_local(location, file_list[i])
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
    #st[0:3].plot()

# read waveform
# print("Reading Waveform...")
# json_traces = mongo_conn.db[collection].find({})
# print(json_traces)
#decoded_wf = stream_db.read_full_waveform(event.resource_id.id)
#print(decoded_wf)
print("Closing Connection...")
mongo_conn.disconnect()


