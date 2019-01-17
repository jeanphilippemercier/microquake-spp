from microquake.core import read_events
from microquake.core.stream import *
from io import BytesIO
import requests
from dateutil import parser
from microquake.core import UTCDateTime
from IPython.core.debugger import Tracer


class RequestEvent:
    def __init__(self, ev_dict):
        for key in ev_dict.keys():
            if 'time' in key:
                if type(ev_dict[key]) is not str:
                    setattr(self, key, ev_dict[key])
                    continue
                setattr(self, key, UTCDateTime(parser.parse(ev_dict[key])))
            else:
                setattr(self, key, ev_dict[key])

    def get_event(self):
        event_file = requests.request('GET', self.event_file)
        return read_events(event_file.content, format='QUAKEML')

    def get_waveforms(self):
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


def post_data_from_files(api_base_url, event_id=None, event_file=None,
                         mseed_file=None, mseed_context_file=None,
                         tolerance=0.5, logger=None):
    """
    Build request directly from objects
    :param api_base_url: base url of the API
    :param event_id: event_id (not required if an event is provided)
    :param event_file: path to a QuakeML file
    :param stream_file: path to a mseed seismogram file
    :param context_stream_file: path to a context seismogram file
    :param tolerance: Minimum time between an event already in the database
    and this event for this event to be inserted into the database. This is to
    avoid duplicates. event with a different
    <event_id> within the <tolerance> seconds of the current object will
    not be inserted. To disable this check set <tolerance> to None.
    :param logger: a logging.Logger object
    :return: same as build_request_data_from_bytes
    """

    __name__ = 'spp.utils.post_data_from_files'

    if logger is None:
        import logging
        logger = logging.getLogger(__name__)

    event=None,
    stream=None
    context_stream=None

    # read event
    if event_file is not None:
        event = read_events(event_file)

    # read waveform
    if mseed_file is not None:
        stream = read(mseed_file, format='MSEED')

    # read context waveform
    if mseed_context_file is not None:
        context_stream = read(mseed_context_file, format='MSEED')

    return post_data_from_objects(api_base_url, event_id=event_id,
                                  event=event, stream=stream,
                                  context_stream=context_stream,
                                  tolerance=tolerance, logger=logger)


def post_data_from_objects(api_base_url, event_id=None, event=None,
                          stream=None, context_stream=None, tolerance=0.5,
                          logger=None):
    """
    Build request directly from objects
    :param api_base_url: base url of the API
    :param event_id: event_id (not required if an event is provided)
    :param event: microquake.core.event.Event or a
    microquake.core.event.Catalog containing a single event. If the catalog
    contains more than one event, only the first event, <catalog[0]> will be
    considered. Use catalog with caution.
    :param stream: event seismogram (microquake.core.Stream.stream)
    :param context_stream: context seismogram trace (
    microquake.core.Stream.stream)
    :param tolerance: Minimum time between an event already in the database
    and this event for this event to be inserted into the database. This is to
    avoid duplicates. event with a different
    <event_id> within the <tolerance> seconds of the current object will
    not be inserted. To disable this check set <tolerance> to None.
    :param logger: a logging.Logger object
    :return: same as build_request_data_from_bytes
    """

    from microquake.core.event import Catalog

    api_url = api_base_url + "events"

    __name__ = 'spp.utils.post_data_from_objects'

    if logger is None:
        import logging
        logger = logging.getLogger(__name__)

    event_bytes=None
    mseed_bytes=None
    mseed_context_bytes=None

    if type(event) is type(Catalog):
        event = event[0]
        logger.warning('a <microquake.core.event.Catalog> object was '
                       'provided, only the first element of the catalog will '
                       'be used, this may lead to an unwanted behavior')

    # check if insertion is possible
    data = {}
    if event is not None:
        event_time = event.preferred_origin().time
        event_resource_id = str(event.resource_id)
    else:
        if event_id is None:
            logger.warning('A valid event_id must be provided when no '
                           '<event> is not provided.')
            logger.info('exiting')
            return

        re = get_event_by_id(api_base_url, event_id)
        if re is None:
            logger.warning('request did not return any event with the '
                           'specified event_id. A valid event_id or an event '
                           'object must be provided to insert a stream or a '
                           'context stream into the database.')
            logger.info('exiting')
            return

        event_time = re.time_utc
        event_resource_id = str(re.event_resource_id)

    data['event_resource_id'] = event_resource_id

    logger.info('processing event with resource_id: %s' % event_resource_id)

    base_event_file_name = str(event_time)
    files = {}
    # Test if there is an event within <tolerance> seconds in the database
    # with a different <event_id>. If so abort the insert

    if tolerance is not None:
        start_time = event_time - tolerance
        end_time = event_time + tolerance
        re_list = get_events_catalog(api_base_url, start_time, end_time)
        if re_list:
            logger.warning('Event found within % seconds of current event'
                           % tolerance)
            logger.warning('The current event will not be inserted into the'
                           ' data base')
            return

    if event is not None:
        logger.info('preparing event data')
        event_bytes = BytesIO()
        event.write(event_bytes, format='QUAKEML')
        event_file_name = base_event_file_name + '.xml'
        event_bytes.name = event_file_name
        event_bytes.seek(0)
        files['event'] = event_bytes
        logger.info('done preparing event data')

    if stream is not None:
        logger.info('preparing waveform data')
        mseed_bytes = BytesIO()
        stream.write(mseed_bytes, format='MSEED')
        mseed_file_name = base_event_file_name + '.mseed'
        mseed_bytes.name = mseed_file_name
        mseed_bytes.seek(0)
        files['waveform'] = mseed_bytes
        logger.info('done preparing waveform data')

    if context_stream is not None:
        logger.info('preparing context waveform data')
        mseed_context_bytes = BytesIO()
        context_stream.write(mseed_context_bytes, format='MSEED')
        mseed_context_file_name = base_event_file_name + '.context_mseed'
        mseed_context_bytes.name = mseed_context_file_name
        mseed_context_bytes.seek(0)
        files['context'] = mseed_context_bytes
        logger.info('done preparing context waveform data')

    return post_event_data(api_url, data, files)


def post_event_data(api_base_url, request_data, request_files):
    url = api_base_url + "events"

    headers = {'Content-Type': 'multipart/form-data'}

    result = requests.post(api_base_url, data=request_data, files=request_files)
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
    """
    return a list of events
    :param api_base_url:
    :param start_time:
    :param end_time:
    :return:
    """
    from IPython.core.debugger import Tracer
    from microquake.core import AttribDict
    url = api_base_url + "catalog"

    # request work in UTC, time will need to be converted from whatever
    # timezone to UTC before the request is built.

    querystring = {"start_time": start_time, "end_time": end_time}

    response = requests.request("GET", url,  params=querystring).json()

    events = []
    for event in response:
        events.append(RequestEvent(event))

    return events


def get_event_by_id(api_base_url, event_resource_id):
    import json

    # smi:local/e7021615-e7f0-40d0-ad39-8ff8dc0edb73
    url = api_base_url + "events"
    querystring = {"event_resource_id": event_resource_id}

    response = requests.request("GET", url, params=querystring)

    if response.status_code != 200:
        return None

    return RequestEvent(json.loads(response.content))


def get_continuous_stream(api_base_url, start_time, end_time, station=None,
                          channel=None, network=None):
    url = api_base_url + "continuous_waveform"


    querystring = {'start_time': str(start_time), 'end_time': str(end_time),
                   "station":station,}

    response = requests.request('GET', url,  params=querystring)
    file = BytesIO(response.content)
    wf = read(file, format='MSEED')
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
