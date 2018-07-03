from microquake.core import read_events, stream, read
from microquake.db.mongo.mongo import MongoDBHandler, EventDB, StreamDB
import yaml
import os
# reload(mongo)

def insert_continuous_db(stream):
    """

    :param stream:
    :return:
    """



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
db = MongoDBHandler(uri=params['uri'], db_name='waveforms')

st = read('../../data/2018-04-15_034422.mseed')



# print("inserting into DB")
# event_db = EventDB(db)
# event_db.insert_event(event, catalog_index=0)
# # read event
# print("Reading Event...")
# json_event = event_db.read_event(event.resource_id.id)
# print(json_event)
# decoded_ev = event_db.read_full_event(event.resource_id.id)
# print(decoded_ev)
#
# # Testing waveform saving and loading
# # read mseed waveform
# wf = stream.read(config_dir + "../data/" + "20170419_153133.mseed")
# # json_wf = json.dumps(wf, default=d)
# print("Waveform:")
# stream_db = StreamDB(db)
# id = stream_db.insert_waveform(wf, event.resource_id.id)
# print(id)
# # read waveform
# print("Reading Waveform...")
# json_waveform = stream_db.read_waveform(event.resource_id.id)
# print(json_waveform)
# decoded_wf = stream_db.read_full_waveform(event.resource_id.id)
# print(decoded_wf)
# print("Closing Connection...")
# db.disconnect()