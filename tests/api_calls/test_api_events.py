import obspy
import urllib.request as urllib_request
import io
import os
#from microquake.core.event import *
from obspy.core.event import read_events
from microquake.core.stream import *
from io import BytesIO
import base64
import urllib
import json

#url='http://40.76.192.141:5000/getStream?starttime=2018-05-23T11:00:18.000&endtime=2018-05-23T11:00:19.00'


timeout=120
headers = {}
handlers = []
url_opener = None

config_dir = os.environ['SPP_CONFIG']

url_base = 'http://localhost:5000/events/'


def build_data():
    # prepare event
    evt = read_events(config_dir + "/../data/" + 'event4.xml') #'2018-04-15_034422.xml')
    #cat = Catalog(events=[evt])
    evt_bytes = BytesIO()
    evt.write(evt_bytes, format="QUAKEML")

    # prepare waveform
    wf = read(config_dir + "/../data/" + 'event.mseed', format='MSEED')
    wf_bytes = BytesIO()
    wf.write(wf_bytes)

    # construct the data dict:
    data = {}
    data['event'] = base64.b64encode(evt_bytes.getvalue()).decode('utf-8')
    data['waveform'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')
    data['context'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')

    #print(data)
    #print(json.dumps(data))
    return json.dumps(data)

def put_event():
    method_url = "putEvent"
    #req_data = urllib.parse.urlencode(build_data()).encode("utf-8")
    req_data = build_data().encode("utf-8")
    request = urllib_request.Request(url=url_base + method_url, data=req_data)
    #url_opener=set_opener(None, None)
    request.add_header("Content-Type", 'application/json')
    url_opener = urllib_request.build_opener()
    result = url_opener.open(request, timeout=timeout)

    if result.code == 200:
        print(result.read())
    else:
        print("Error...!!", result.code)

if __name__ == "__main__":

    put_event()
