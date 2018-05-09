from pytz import tz
from dateutil.tz import tzoffset
import json
import os

def read_config():
    
    fname = os.environ['SPP_CONFIG'] + 'time.json'
    params = json.load(fname)