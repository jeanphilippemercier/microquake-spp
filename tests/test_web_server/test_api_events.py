import obspy
import urllib.request as urllib_request
import io
import os
#from microquake.core.event import *
from obspy.core.event import read_events
#from microquake.core import read_events
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


def build_event_data(event_file, mseed_file, mseed_context_file):
    # prepare event
    #evt = read_events(config_dir + "/../data/" + 'event4.xml') #'2018-04-15_034422.xml')
    evt = read_events(event_file)
    #cat = Catalog(events=[evt])
    evt_bytes = BytesIO()
    evt.write(evt_bytes, format="QUAKEML")

    # prepare waveform
    wf = read(mseed_file, format='MSEED')
    #wf = read(config_dir + "/../data/" + 'event.mseed', format='MSEED')
    wf_bytes = BytesIO()
    wf.write(wf_bytes)

    # prepare context waveform
    wfc = read(mseed_context_file, format='MSEED')
    wfc_bytes = BytesIO()
    wfc.write(wfc_bytes)


    # construct the data dict:
    data = {}
    data['event']    = base64.b64encode(evt_bytes.getvalue()).decode('utf-8')
    data['waveform'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')
    data['context']  = base64.b64encode(wfc_bytes.getvalue()).decode('utf-8')

    #print(data)
    #print(json.dumps(data))
    return json.dumps(data).encode("utf-8")

def build_update_event_data(event_id, event_file, mseed_file, mseed_context_file):
    evt = read_events(event_file)
    evt_bytes = BytesIO()
    evt.write(evt_bytes, format="QUAKEML")

    # prepare waveform
    wf = read(mseed_file, format='MSEED')
    wf_bytes = BytesIO()
    wf.write(wf_bytes)

    # prepare context waveform
    wfc = read(mseed_context_file, format='MSEED')
    wfc_bytes = BytesIO()
    wfc.write(wfc_bytes)

    # construct the data dict:
    data = {}
    data['event_id'] = event_id
    data['event']    = base64.b64encode(evt_bytes.getvalue()).decode('utf-8')
    data['waveform'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')
    data['context']  = base64.b64encode(wfc_bytes.getvalue()).decode('utf-8')

    return json.dumps(data).encode("utf-8")


def build_update_event_data2(event_id):
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
    data['event_id'] = event_id

    #data['event'] = base64.b64encode(evt_bytes.getvalue()).decode('utf-8')
    data['waveform'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')
    #data['context'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')

    #print(data)
    #print(json.dumps(data))
    return json.dumps(data).encode("utf-8")

def build_event_inuse_data():

    data = {}
    data['eventid'] = "5b59ec5da949fef9a73dff06"
    data['userid'] = "5b59ec5da949fef9a73dff00"

    return json.dumps(data).encode("utf-8")

def post_data(method_url, request_data):
    #print('** post_data: url_base=%s + method_url=%s = [%s]' % (url_base, method_url, url_base + method_url))
    request = urllib_request.Request(url=url_base + method_url, data=request_data)
    #url_opener=set_opener(None, None)
    request.add_header("Content-Type", 'application/json')
    url_opener = urllib_request.build_opener()
    result = url_opener.open(request, timeout=timeout)

    '''
    if result.code == 200:
        #print(result.read())
        print('I wont consume it!')
    else:
        print("Error...!!", result.code)
        print("Error Message:", result.read())
    '''
    return result

if __name__ == "__main__":

    #post_data("putEvent", build_event_data())

    post_data("updateEvent", build_update_event_data('5b66243da949fe1c0e6aa471'))

    #post_data("putEventInUse", build_event_inuse_data())
