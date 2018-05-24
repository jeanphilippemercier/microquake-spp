from microquake.core import read_events,stream
from microquake.db.mongo.mongo import MongoHandler
import yaml
import os
# reload(mongo)

# reading the config file

config_dir = os.environ['SPP_CONFIG']
config_file = os.path.join(config_dir, 'permanent_db.yaml')

with open(config_file,'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['db']

# Testing saving and loading Event
cat = read_events(config_dir + "../data/" + 'nll_processed.xml')
event = cat[10]

print("connecting to DB")
db = MongoHandler(uri=params['uri'], db_name=params['db_name'])
print("inserting into DB")
db.insert_event(event, catalog_index=0)
# read event
print("Reading Event...")
json_event = db.read_event(event.resource_id.id)
print(json_event)
decoded_ev = db.read_full_event(event.resource_id.id)
print(decoded_ev)

# Testing waveform saving and loading
# read mseed waveform
wf = stream.read(config_dir + "../data/" + "20170419_153133.mseed")
# json_wf = json.dumps(wf, default=d)
print("Waveform:")
id = db.insert_waveform(wf, event.resource_id.id)
print(id)
# read waveform
print("Reading Waveform...")
json_waveform = db.read_waveform(event.resource_id.id)
print(json_waveform)
decoded_wf = db.read_full_waveform(event.resource_id.id)
print(decoded_wf)


