from microquake.core import read_events,stream
from microquake.db.mongo import mongo


# Testing saving and loading Event
cat = read_events('nll_processed.xml')
print("connecting to DB")
db = mongo.connect(uri='mongodb://localhost:27017/', db_name='test')
print("inserting into DB")
mongo.insert_event(db, cat[-1], 'h', 'hh', '10', catalog_index=0)
# read event
print("Reading Event...")
json_event = mongo.read_event(db,'h','hh')
print(json_event)
decoded_ev = mongo.read_full_event(db,'h','hh')
print(decoded_ev)

# Testing waveform saving and loading
# read mseed waveform
wf = stream.read("20170419_153133.mseed")
# json_wf = json.dumps(wf, default=d)
print("Waveform:")
id = mongo.insert_waveform(db, wf, 'h', 'hh')
print(id)
# read waveform
print("Reading Waveform...")
json_waveform = mongo.read_waveform(db,'h','hh')
print(json_waveform)
decoded_wf = mongo.read_full_waveform(db,'h','hh')
print(decoded_wf)


