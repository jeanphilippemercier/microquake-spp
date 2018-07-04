from microquake.core import read_events,stream
from microquake.db.mongo.mongo import MongoDBHandler, EventDB, StreamDB
import yaml
import os
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

# read mseed waveform
wf = stream.read(config_dir + "/../data/" + "20170419_153133.mseed")
# json_wf = json.dumps(wf, default=d)
print("Waveform:")
traces_list = StreamDB.encode_stream(wf)
mongo_conn.db.traces.insert_many(traces_list)

# read waveform
print("Reading Waveform...")
json_traces = mongo_conn.db.traces.find({})
print(json_traces)
#decoded_wf = stream_db.read_full_waveform(event.resource_id.id)
#print(decoded_wf)
print("Closing Connection...")
mongo_conn.disconnect()


