from microquake.core import read_events
from microquake.db import mongo

cat = read_events('nll_processed.xml')
db = mongo.connect(uri='mongodb://localhost:27017/', db_name='test')

db = mongo.connect(uri='mongodb://localhost:27017/', db_name='test')
mongo.insert_event(db, cat[-1], '', '', '', catalog_index=0)
