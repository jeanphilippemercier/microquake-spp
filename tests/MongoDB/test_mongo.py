from microquake.core import read_events,stream
from microquake.db.mongo import mongo
reload(mongo)

# Testing saving and loading Event
cat = read_events('nll_processed.xml')
event = cat[10]
print("connecting to DB")
db = mongo.connect(uri='mongodb://localhost:27017/', db_name='test')
print("inserting into DB")
wf=stream.read("20170419_153133.mseed")
mongo.insert_event(db, event, catalog_index=0)
# read event
print("Reading Event...")
json_event = mongo.read_event(db, event.resource_id.id)
print(json_event)
decoded_ev = mongo.read_full_event(db, event.resource_id.id)
print(decoded_ev)

# Testing waveform saving and loading
# read mseed waveform
wf = stream.read("20170419_153133.mseed")
# json_wf = json.dumps(wf, default=d)
print("Waveform:")
id = mongo.insert_waveform(db, wf, event.resource_id.id)
print(id)
# read waveform
print("Reading Waveform...")
json_waveform = mongo.read_waveform(db, event.resource_id.id)
print(json_waveform)
decoded_wf = mongo.read_full_waveform(db, event.resource_id.id)
print(decoded_wf)


