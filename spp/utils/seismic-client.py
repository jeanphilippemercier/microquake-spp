import urllib.request as urllib_request
import io
import os
from microquake.core import read_events
from microquake.core.stream import *
from io import BytesIO
import base64
import urllib
import json
import requests
from spp.utils.application import Application

# config_dir = os.environ['SPP_CONFIG']


# url_base = 'http://localhost:5000/events/'


def build_request_data_from_files(event_id, event_file, mseed_file, mseed_context_file):
    files = dict()

    # prepare event
    if event_file is not None:
        evt = read_events(event_file)

        if event_id is None:
            event_id = evt[0].resource_id.id

        evt_bytes = BytesIO()
        evt.write(evt_bytes, format="QUAKEML")
        evt_bytes.name = event_file
        evt_bytes.seek(0)
        files['event'] = evt_bytes

    # prepare waveform
    if mseed_file is not None:
        wf = read(mseed_file, format='MSEED')
        wf_bytes = BytesIO()
        wf.write(wf_bytes)
        wf_bytes.name = mseed_file
        wf_bytes.seek(0)
        files['waveform'] = wf_bytes

    # prepare context waveform
    if mseed_context_file is not None:
        wfc = read(mseed_context_file, format='MSEED')
        wfc_bytes = BytesIO()
        wfc.write(wfc_bytes)
        wfc_bytes.name = mseed_context_file
        wfc_bytes.seek(0)
        files['context'] = wfc_bytes

    data = dict()
    data['event_resource_id'] = str(event_id)

    print(data, files)

    return data, files


def build_request_data_from_bytes(event_id, event_bytes, mseed_bytes, mseed_context_bytes):
    files = dict()

    # prepare event
    if event_bytes is not None:
        files['event'] = event_bytes

    # prepare waveform
    if mseed_bytes is not None:
        files['waveform'] = mseed_bytes

    # prepare context waveform
    if mseed_context_bytes is not None:
        files['context'] = mseed_context_bytes

    data = dict()
    data['event_resource_id'] = str(event_id)

    print(data)

    return data, files


def post_event_data(api_base_url, request_data, request_files):
    url = api_base_url + "events"

    headers = {'Content-Type': 'multipart/form-data'}

    result = requests.post(url, data=request_data, files=request_files)
    print(result)

    '''
    if result.code == 200:
        #print(result.read())
        print('I wont consume it!')
    else:
        print("Error...!!", result.code)
        print("Error Message:", result.read())
    '''
    return result


def get_events_catalog(api_base_url, start_time, end_time):
    url = api_base_url + "catalog"

    querystring = {"start_time": start_time, "end_time": end_time}

    response = requests.request("GET", url,  params=querystring)
    print(response.text)
    return response.text


if __name__ == "__main__":

    app = Application()

    API_BASE_URL = app.settings.API.base_url

    data, files = build_request_data_from_files( None
                                                , 'data/2018-11-08T11-16-47.968400Z.xml'
                                                ,'data/2018-11-08T11-16-47.968400Z.mseed'
                                                , None #'20180523_125102207781.mseed_context'
                                                )

    post_event_data(API_BASE_URL, data, files)

    get_events_catalog(API_BASE_URL, "2018-11-08T10:21:48.898496Z", "2018-11-08T11:21:49.898496Z")