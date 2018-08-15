import obspy
import urllib.request as urllib_request
import io

#url='http://40.76.192.141:5000/getStream?starttime=2018-05-23T11:00:18.000&endtime=2018-05-23T11:00:19.00'

timeout=420
headers = {}
handlers = []
url_opener = None

#url_base = 'http://40.76.192.141:5000'
url_base = 'http://localhost:5000'

import logging
logger = logging.getLogger()

def get_url(starttime=None, endtime=None, **kwargs):
    # start/end are required
    start_time = str(starttime)
    end_time   = str(endtime)

    url = '%s/getStream?starttime=%s&endtime=%s' % (url_base, start_time, end_time)

    #MTH: the params are all expected to be strings without quotation
    if 'net' in kwargs:
        url += '&net=%s' % kwargs['net']
    if 'sta' in kwargs:
        #url += '&sta=\"%s\"' % kwargs['sta']
        url += '&sta=%s' % kwargs['sta']
    if 'cha' in kwargs:
        url += '&cha=%s' % kwargs['cha']
        #url += '&cha=\"%s\"' % kwargs['cha']
    #print('get_url: [%s]' % url)
    #exit()

    return(url)


def set_opener(user, password):
    # Only add the authentication handler if required.
    handlers = []
    if user is not None and password is not None:
        # Create an OpenerDirector for HTTP Digest Authentication
        password_mgr = urllib_request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, self.base_url, user, password)
        handlers.append(urllib_request.HTTPDigestAuthHandler(password_mgr))

    if (user is None and password is None) or self._force_redirect is True:
        # Redirect if no credentials are given or the force_redirect
        # flag is True.
        #handlers.append(CustomRedirectHandler())
        pass
    else:
        handlers.append(NoRedirectionHandler())

    # Don't install globally to not mess with other codes.
    url_opener = urllib_request.build_opener(*handlers)
    print('Installed new opener with handlers: {!s}'.format(handlers))
    return url_opener

def get_stream(url):
    request = urllib_request.Request(url=url, headers=headers)
    #url_opener=set_opener(None, None)
    url_opener = urllib_request.build_opener()
    url_obj = url_opener.open(request, timeout=timeout)
    #print(type(url_obj))

    result = url_obj.read()
    try:
        data_stream = io.BytesIO(result)
        data_stream.seek(0, 0)
        st = obspy.read(data_stream, format="MSEED")
        data_stream.close()
        return(st)
    except:
#obspy.io.mseed.ObsPyMSEEDFilesizeTooSmallError
        #raise
#UnicodeDecodeError:
        read_string  = result.decode('utf-8')
        d = json.loads(read_string)
        if 'no_data_found' in d:
            print('No data found!')
        return None


def get_stream_from_mongo(starttime, endtime, **kwargs):
    url = get_url(starttime, endtime, **kwargs)
    st = get_stream(url)
    logger.info('get_stream_from_mongo: server get_stream returned type:%s' % type(st))
    return(st)


import urllib.request as urllib_request
import io
import os
from microquake.core import read_events
from microquake.core.stream import *
from io import BytesIO
import base64
import urllib
import json

timeout=120
headers = {}
handlers = []
url_opener = None

config_dir = os.environ['SPP_CONFIG']

#url_base = 'http://localhost:5000/events/'

def build_event_data(event_file, mseed_file, mseed_context_file):
    # prepare event
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
    data['event']    = base64.b64encode(evt_bytes.getvalue()).decode('utf-8')
    data['waveform'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')
    data['context']  = base64.b64encode(wfc_bytes.getvalue()).decode('utf-8')

    return json.dumps(data).encode("utf-8")

def build_update_event_data(event_id, event_file=None, mseed_file=None, mseed_context_file=None):

    data = {}
    data['event_id'] = event_id

    if event_file:
        evt = read_events(event_file)
        evt_bytes = BytesIO()
        evt.write(evt_bytes, format="QUAKEML")
        data['event']    = base64.b64encode(evt_bytes.getvalue()).decode('utf-8')

    # prepare waveform
    if mseed_file:
        wf = read(mseed_file, format='MSEED')
        wf_bytes = BytesIO()
        wf.write(wf_bytes)
        data['waveform'] = base64.b64encode(wf_bytes.getvalue()).decode('utf-8')

    # prepare context waveform
    if mseed_context_file:
        wfc = read(mseed_context_file, format='MSEED')
        wfc_bytes = BytesIO()
        wfc.write(wfc_bytes)
        data['context']  = base64.b64encode(wfc_bytes.getvalue()).decode('utf-8')

    return json.dumps(data).encode("utf-8")


def build_event_inuse_data():

    data = {}
    data['eventid'] = "5b59ec5da949fef9a73dff06"
    data['userid'] = "5b59ec5da949fef9a73dff00"

    return json.dumps(data).encode("utf-8")

def post_data(method_url, request_data):
    #print('** post_data: url_base=%s + method_url=%s = [%s]' % (url_base, method_url, url_base + method_url))
    #print('url_base=[%s] + method_url=[%s] --> [%s]' % (url_base, method_url, url_base + '/events/' + method_url))
    #exit()

    request = urllib_request.Request(url=url_base + '/events/' + method_url, data=request_data)
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
