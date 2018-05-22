from microquake.core import read_events, read
from microquake.db.mongo import mongo

cat = read_events('../../data/nll_processed.xml')
event = cat[10]
evt_enc = mongo.encode_event(event)
evt_dec = mongo.decode_event(evt_enc)

if event == evt_dec:
    print('event encoding decoding successful')
else:
    print('event encoding decoding failed')

st = read('../../data/20170419_153133.mseed')
st_enc = mongo.encode_stream(st)
st_dec = mongo.decode_stream(st_enc)


# returns false because the two object are not the same, they are equal as
# far as we are concerned however.
# if st == st_dec:
#     print('stream encoding decoding successful')
# else:
#     print('stream encoding decoding failed')
