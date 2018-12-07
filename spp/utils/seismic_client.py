from microquake.core import read_events
from microquake.core.stream import *
from io import BytesIO
import requests


class RequestEvent:
    def __init__(self, ev_dict):
        for key in ev_dict.keys():
            setattr(self, key, ev_dict[key])

    def get_event(self):
        event_file = requests.request('GET', self.event_file)
        return read_events(event_file.content, format='QUAKEML')

    def get_waveform(self):
        waveform_file = requests.request('GET', self.waveform_file)
        byte_stream = BytesIO(waveform_file.content)
        return read(byte_stream, format='MSEED')

    def get_context(self):
        waveform_context_file = requests.request('GET',
                                                 self.context_waveform_file)
        byte_stream = BytesIO(waveform_context_file)
        return read(byte_stream)

    def select(self):
        # select by different attributes
        # TODO write this function :-)
        pass

    def keys(self):
        return self.__dict__.keys()


def build_request_data_from_files(event_id, event_file, mseed_file,
                                  mseed_context_file):
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


def build_request_data_from_object(event_id=None, event=None, stream=None,
                                   context_stream=None):
    """
    Build request directly from object
    :param event_id: event_id (not required if an event is provided)
    :param event: microquake.core.Event.event
    :param stream: event seismogram (microquake.core.Stream.stream)
    :param context_stream: context seismogram trace (
    microquake.core.Stream.stream)
    :return: same as build_request_data_from_bytes
    """

    event_id=None
    event_bytes=None
    mseed_bytes=None
    mseed_context_bytes=None

    if event is not None:
        ev_io = BytesIO()
        event.write(ev_io, format='QUAKEML')
        event_bytes = ev_io.getvalue()

    if stream is not None:
        st_io = BytesIO()
        stream.write(st_io, format='MSEED')
        mseed_bytes = st_io.getvalue()

    if context_stream is not None:
        st_c_io = BytesIO()
        context_stream.write(st_c_io, format='MSEED')
        mseed_context_bytes = st_c_io.getvalue()

    return build_request_data_from_bytes(event_id, event_bytes, mseed_bytes,
                                         mseed_context_bytes)


def build_request_data_from_bytes(event_id, event_bytes, mseed_bytes,
                                  mseed_context_bytes):
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
    from IPython.core.debugger import Tracer
    from microquake.core import AttribDict
    url = api_base_url + "catalog"

    querystring = {"start_time": start_time, "end_time": end_time}

    response = requests.request("GET", url,  params=querystring).json()

    events = []
    for event in response:
        events.append(RequestEvent(event))

    return events


def get_continuous_stream(api_base_url, start_time, end_time):
    url = api_base_url + "continuous_waveform"

    querystring = {"start_time": start_time, "end_time": end_time}

    response = requests.request("GET", url,  params=querystring)
    file = BytesIO(response.content)
    wf = read(file, format='MSEED')
    print(wf)
    return wf


def post_continuous_stream(api_base_url, stream, post_to_kafka=True,
                           stream_id=None):
    url = api_base_url + "continuous_waveform_upload"

    request_files = {}
    wf_bytes = BytesIO()
    stream.write(wf_bytes, format='MSEED')
    wf_bytes.name = stream_id
    wf_bytes.seek(0)
    request_files['continuous_waveform_file'] = wf_bytes

    request_data = {}
    if post_to_kafka:
        request_data['destination'] = 'kafka'
    else:
        request_data['destination'] = 'db'

    if stream_id is not None:
        request_data['stream_id'] = stream_id
    else:
        from uuid import uuid4
        request_data['stream_id'] = uuid4()

    result = requests.post(url, data=request_data, files=request_files)
    print(result)


# if __name__ == "__main__":
#
#     app = Application()
#
#     API_BASE_URL = app.settings.API.base_url
#
#     data, files = build_request_data_from_files( None
#                                                 ,'data/2018-11-08T11-16-47.968400Z.xml'
#                                                 ,'data/2018-11-08T11-16-47.968400Z.mseed'
#                                                 , None #'20180523_125102207781.mseed_context'
#                                                 )
#
#     post_event_data(API_BASE_URL, data, files)
#
#     get_events_catalog(API_BASE_URL, "2018-11-08T10:21:48.898496Z",
#                        "2018-11-08T11:21:49.898496Z")
