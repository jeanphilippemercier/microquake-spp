from flask import Flask, request, jsonify, send_file
import os
import sys
from microquake.db.mongo.mongo import MongoDBHandler, StreamDB, EventDB, MongoJSONEncoder
from microquake.core.stream import Stream
from microquake.core import read
from io import BytesIO
import time
from obspy.core.utcdatetime import UTCDateTime
import numpy as np
from obspy.core.event import read_events
from microquake.core.event import Event
import base64
from spp.utils.config import Configuration
from bson.objectid import ObjectId
import json


app = Flask(__name__)

# load configuration file
config = Configuration()

mongo = MongoDBHandler(config.DB_CONFIG['uri'], config.DB_CONFIG['db_name'])

TRACES_COLLECTION = config.DB_CONFIG['traces_collection']
EVENTS_COLLECTION = config.DB_CONFIG['events_collection']
EVENTS_INUSE_COLLECTION = config.DB_CONFIG['events_inuse_collection']
INUSE_TTL = int(config.DB_CONFIG['inuse_ttl'])
BASE_DIR = config.DB_CONFIG['filestore_base_dir']

@app.route('/', methods=['GET'])
def home():
    return '''<h1>Ingestion MSEED Data Retrieval</h1>
<p></p>'''


def construct_output(stream_obj, requested_format='MSEED'):
    output = None
    output_size = 0
    start_time = time.time()

    if requested_format == "MSEED":
        output = BytesIO()
        stream_obj.write(output, format="MSEED")
        output_size = sys.getsizeof(output.getvalue())
        output.seek(0)

    end_time = time.time() - start_time
    print("==> Formatting Output took: ", "%.2f" % end_time, "Output Size:", "%.2f" % (output_size/1024/1024), "MB")
    return output


def combine_json_to_stream_data(db_result):
    start_time = time.time()
    s = Stream.create_from_json_traces(db_result)
    records_count = len(s.traces)
    merged_stream = s.merge(fill_value=0, method=1)
    end_time = time.time() - start_time
    print("==> DB Fetching and Stream Creation took:", "%.2f" % end_time, ", Records Count:", records_count,
          ", Merged Traces Count:", len(s.traces))
    return merged_stream


def check_and_parse_datetime(dt_str):
    try:
        dt = int(np.float64(UTCDateTime(dt_str).timestamp) * 1e9)
        return dt
    except ValueError:
        error_msg = "Invalid datetime format, value should be in format: %Y-%m-%dT%H:%M:%S.%f"
        print(error_msg)
        raise InvalidUsage(error_msg, status_code=410)


@app.route('/getStream', methods=['GET'])
def get_stream():

    request_starttime = time.time()

    network = None
    station = None
    channel = None
    output_formats = ["MSEED", "PLOT", "BINARY"]
    # make default output format is MSEED
    output_format = "MSEED"

    # Validate Request Params
    # Check Date Ranges
    if 'starttime' in request.args and ('endtime' in request.args or 'duration' in request.args):
        print('get_stream: starttime:%s endtime:%s' % (request.args['starttime'], request.args['endtime']))

        start_time = check_and_parse_datetime(request.args['starttime'])
        print(start_time)
        if 'endtime' in request.args:
            end_time = check_and_parse_datetime(request.args['endtime'])
        else:
            end_time = start_time + (int(request.args['duration']) * 1000000000)

        print(end_time)
    else:
        raise InvalidUsage("date-range-options must be specified like:" +
                           "(starttime=<time>) & ([endtime=<time>] | [duration=<seconds>])",
                           status_code=411)

    # Other Optional
    if 'net' in request.args:
        network = request.args['net']

    if 'sta' in request.args:
        station = request.args['sta']

    if 'cha' in request.args:
        channel = request.args['cha']

    if 'output' in request.args:
        if request.args['output'] in output_formats:
            output_format = request.args['output']
        else:
            raise InvalidUsage("Invalid output option, should be one of these:" +
                               "MSEED, PLOT, BINARY",
                               status_code=411)

    criteria_filter = construct_filter_criteria(start_time, end_time, network, station, channel)

    result = mongo.db[TRACES_COLLECTION].find(criteria_filter, {"_id": 0})

    # combine traces together
    resulted_stream = combine_json_to_stream_data(result)

    start_time = UTCDateTime(request.args['starttime'])
    end_time = UTCDateTime(request.args['endtime'])
    resulted_stream.trim(start_time, end_time, pad=True, fill_value=0.0)

    final_output = construct_output(resulted_stream, output_format)

    request_endtime = time.time() - request_starttime
    print("=======> Request Done Successfully.",
          "Total API Request took: ", "%.2f" % request_endtime, "seconds")

    return send_file(final_output, attachment_filename="testing.mseed", as_attachment=True)


def construct_filter_criteria(start_time, end_time, network, station, channel):

    filter = {
        'stats.starttime': {
            '$lte': end_time
         },
        'stats.endtime': {
            '$gte': start_time
         }
    }

    if network is not None:
        filter['stats.network'] = network

    if station is not None and station != 'ALL':
        filter['stats.station'] = station

    if channel is not None and channel != 'ALL':
        filter['stats.channel'] = channel

    print("Filter:", filter)
    return filter


def generate_filename_from_date(dt):
    return dt.strftime("%Y%m%d_%H%M%S%f")


def generate_filepath_from_date(dt):
    return dt.strftime("/%Y/%m/%d/%H/")


def create_filestore_directories(basedir, filepath):
    events_dir = basedir + "events" + filepath
    waveforms_dir = basedir + "waveforms" + filepath

    # create directory in events if not exists
    if not os.path.exists(events_dir):
        os.makedirs(events_dir)
    # create directory in events if not exists
    if not os.path.exists(waveforms_dir):
        os.makedirs(waveforms_dir)


def construct_relative_files_paths(filepath, filename):
    # events
    event_filepath = "events" + filepath + filename + ".xml"
    waveform_filepath = "waveforms" + filepath + filename + ".mseed"
    waveform_context_filepath= "waveforms" + filepath + filename + ".mseed_context"
    return event_filepath, waveform_filepath, waveform_context_filepath





@app.route('/events/putEvent', methods=['POST'])
def put_event():

    request_starttime = time.time()

    if 'event' and 'waveform' and 'context' in request.get_json():
        conversion_starttime = time.time()
        event = read_events(BytesIO(base64.b64decode(request.get_json()['event'])))[0]
        waveform = read(BytesIO(base64.b64decode(request.get_json()['waveform'])), format='MSEED')
        waveform_context = read(BytesIO(base64.b64decode(request.get_json()['context'])), format='MSEED')
        conversion_endtime = time.time() - conversion_starttime
        print("=======> Conversion took: ", "%.2f" % conversion_endtime, "seconds")
    else:
        raise InvalidUsage("Wrong data sent..!! Event, Waveform and Context must be specified in request body",
                           status_code=411)

    ev_flat_dict = EventDB.flatten_event(Event(event))
    print(ev_flat_dict)
    filename = generate_filename_from_date(ev_flat_dict['time'])
    filepath = generate_filepath_from_date(ev_flat_dict['time'])
    print(filepath + filename)

    event_filepath, waveform_filepath, waveform_context_filepath = construct_relative_files_paths(filepath, filename)

    ev_flat_dict['filename'] = filename
    ev_flat_dict['event_filepath'] = event_filepath
    ev_flat_dict['waveform_filepath'] = waveform_filepath
    ev_flat_dict['waveform_context_filepath'] = waveform_context_filepath

    # write files in filestore
    event.write(BASE_DIR + event_filepath, format="QUAKEML")
    waveform.write(BASE_DIR + waveform_filepath, format="MSEED")
    waveform_context.write(BASE_DIR + waveform_context_filepath, format="MSEED")

    inserted_event_id = mongo.db[EVENTS_COLLECTION].insert_one(ev_flat_dict).inserted_id

    print("Event Insertes with ID:", inserted_event_id)

    request_endtime = time.time() - request_starttime
    print("=======> Request Done Successfully.",
          "Total API Request took: ", "%.2f" % request_endtime, "seconds")

    return str(inserted_event_id)


def read_and_write_file_as_bytes(relative_filepath, format):
    bytes = BytesIO()
    obj = None

    #construct full filepath
    full_filepath = BASE_DIR + relative_filepath

    if format =='QUAKEML':
        obj = read_events(full_filepath)
        obj.write(bytes, format="QUAKEML")
    elif format in ['MSEED', 'MSEED_CONTEXT']:
        obj = read(full_filepath, format='MSEED')
        obj.write(bytes)
    return bytes


def construct_event_output(event_data, requested_format):
    output = None
    output_size = 0
    start_time = time.time()

    if requested_format == "QUAKEML":
        output = read_and_write_file_as_bytes(event_data['event_filepath'], requested_format)
    elif requested_format == "MSEED":
        output = read_and_write_file_as_bytes(event_data['waveform_filepath'], requested_format)
    elif requested_format == "MSEED_CONTEXT":
        output = read_and_write_file_as_bytes(event_data['waveform_context_filepath'], requested_format)

    output_size = sys.getsizeof(output.getvalue())
    output.seek(0)
    end_time = time.time() - start_time
    print("==> Formatting Output took: ", "%.2f" % end_time, "Output Size:", "%.2f" % (output_size/1024/1024), "MB")
    return output


@app.route('/events/getEvent', methods=['GET'])
def get_event():

    request_starttime = time.time()

    output_types = {'MSEED': ".mseed",
                    'MSEED_CONTEXT': ".mseed",
                    'QUAKEML': ".xml",
                    'ALL': '.gzip'}

    # Check Date Ranges
    if 'eventid' in request.args:
        print('get_Event: eventid:%s' % request.args['eventid'])
        event_id = request.args['eventid']
    else:
        raise InvalidUsage("Event ID must be specified like:" +
                           "eventid=<ID>",
                           status_code=411)

    if 'output' in request.args:
        if request.args['output'] in output_types.keys():
            output_format = request.args['output']
        else:
            raise InvalidUsage("Invalid output option, should be one of these:" +
                               "MSEED, MSEED_CONTEXT, QUAKEML",
                               status_code=411)
    else:
        output_format = "ALL"

    query_filter = {"_id": ObjectId(event_id)}

    event_result = mongo.db[EVENTS_COLLECTION].find(query_filter, {"_id": 0})

    if event_result.count() == 1:

        output_file = construct_event_output(event_result[0], output_format)
        # construct downloaded filename
        output_filename = event_result[0]['filename'] + output_types[output_format]

        request_endtime = time.time() - request_starttime
        print("=======> Request Done Successfully.",
              "Total API Request took: ", "%.2f" % request_endtime, "seconds")

        return send_file(output_file, attachment_filename=output_filename, as_attachment=True)

    else:
        return json.dumps({})


@app.route('/events/getEventInUse', methods=['GET'])
def get_event_inuse():

    request_starttime = time.time()

    # Check Date Ranges
    if 'eventid' and 'userid' in request.args:
        print('get_Event_InUse: eventid:%s userid:%s' % (request.args['eventid'], request.args['userid']))
        event_id = request.args['eventid']
        user_id = request.args['userid']
    else:
        raise InvalidUsage("Event ID and User ID must be specified like:" +
                           "(eventid=<ID>) & (userid=<ID>)",
                           status_code=411)

    # get current timestamp in order to fetch inuse events till now
    current_timestamp = time.time() * 10e9

    query_filter = {
        "event_id": ObjectId(event_id),
        "user_id": ObjectId(user_id),
        "ttl_expiration": {'$gte': current_timestamp}
        }

    inuse_result = mongo.db[EVENTS_INUSE_COLLECTION].find(query_filter)

    request_endtime = time.time() - request_starttime
    print("=======> Request Done Successfully.",
          "Total API Request took: ", "%.2f" % request_endtime, "seconds")

    if inuse_result.count() >= 1:
        return MongoJSONEncoder().encode(inuse_result[0])
    else:
        return json.dumps({})


@app.route('/events/putEventInUse', methods=['POST'])
def put_event_inuse():

    request_starttime = time.time()

    # Check Date Ranges
    if 'eventid' and 'userid' in request.get_json():
        print('put_Event_InUse: eventid:%s userid:%s' % (request.get_json()['eventid'], request.get_json()['userid']))
        event_id = request.get_json()['eventid']
        user_id = request.get_json()['userid']
    else:
        raise InvalidUsage("Event ID and User ID must be specified in request body",
                           status_code=411)

    # get current timestamp in order to calculate inuse TTL expiration time
    ttl_expiration = (time.time() + INUSE_TTL) * 10e9

    inuse_data = {
        "event_id": ObjectId(event_id),
        "user_id": ObjectId(user_id),
        "ttl_expiration": ttl_expiration
        }

    inserted_record_id = mongo.db[EVENTS_INUSE_COLLECTION].insert_one(inuse_data).inserted_id

    request_endtime = time.time() - request_starttime
    print("=======> Request Done Successfully.",
          "Total API Request took: ", "%.2f" % request_endtime, "seconds")

    return json.dumps({"event_inuse_id": str(inserted_record_id)})


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The URL is invalid, please check the URL requested</p>", 404


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

if __name__ == '__main__':

    # launch API
    #app.run(debug=True)
    app.run(host='0.0.0.0', threaded=True)
