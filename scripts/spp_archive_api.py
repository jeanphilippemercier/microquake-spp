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
#from obspy.core.event import read_events
from microquake.core import read_events
from microquake.core.event import Event
import base64
from spp.utils.config import Configuration
from bson.objectid import ObjectId
import json
import zipfile
import tarfile
from spp.utils import logger
import datetime
import re


app = Flask(__name__)

log = logger.get_logger("SPP API", 'spp_api.log')

# load configuration file
config = Configuration()

mongo = MongoDBHandler(config.DB_CONFIG['uri'], config.DB_CONFIG['db_name'])

TRACES_COLLECTION = config.DB_CONFIG['traces_collection']
EVENTS_COLLECTION = config.DB_CONFIG['events_collection']
EVENTS_INUSE_COLLECTION = config.DB_CONFIG['events_inuse_collection']
INUSE_TTL = int(config.DB_CONFIG['inuse_ttl'])
BASE_DIR = config.DB_CONFIG['filestore_base_dir']

OUTPUT_TYPES = {'MSEED': ".mseed",
                'MSEED_CONTEXT': ".mseed_context",
                'QUAKEML': ".xml",
                'ALL': '.tar.gz'}


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
    log.info("Formatting Output took: %.2f , Output Size: %.2f MB" % (end_time, (output_size/1024/1024)))
    return output


def combine_json_to_stream_data(db_result):
    start_time = time.time()
    s = Stream.create_from_json_traces(db_result)

    #for tr in s:
        #log.info("combine_json_to_stream: id:%s st:%s et:%s (n:%d) sr:%f" % \
                #(tr.get_id(), tr.stats.starttime, tr.stats.endtime, tr.stats.npts, tr.stats.sampling_rate)) 
    records_count = len(s.traces)
    merged_stream = s.merge(fill_value=0, method=1)
    end_time = time.time() - start_time
    log.info("DB Fetching and Stream Creation took:%.2f , Records Count: %s, Merged Traces Count: %s" % \
            (end_time, records_count, len(s.traces)))
    return merged_stream


def check_and_parse_datetime(dt_str):
    try:
        dt = int(np.float64(UTCDateTime(dt_str).timestamp) * 1e9)
        return dt
    except ValueError:
        error_msg = "Invalid datetime format, value should be in format: %Y-%m-%dT%H:%M:%S.%f"
        log.exception(error_msg)
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

        start_time = check_and_parse_datetime(request.args['starttime'])
        if 'endtime' in request.args:
            end_time = check_and_parse_datetime(request.args['endtime'])
        else:
            end_time = start_time + (int(request.args['duration']) * 1000000000)

        st = UTCDateTime(start_time / 1e9)
        et = UTCDateTime(end_time / 1e9)
        log.info('get_stream: starttime:%s endtime:%s' % (st, et))
    else:
        raise InvalidUsage("date-range-options must be specified like:" +
                           "(starttime=<time>) & ([endtime=<time>] | [duration=<seconds>])",
                           status_code=400)

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
                               status_code=400)

    criteria_filter = construct_filter_criteria(start_time, end_time, network, station, channel)

    result = mongo.db[TRACES_COLLECTION].find(criteria_filter, {"_id": 0})

    if result.count() >= 1:
        # combine traces together
        resulted_stream = combine_json_to_stream_data(result)
        '''
        log.info("MTH: get_stream: resulted_stream type=%s" % (type(resulted_stream)))
        for i,tr in enumerate(resulted_stream):
            stats = tr.stats
            log.info("[%3d] sta:%3s ch:%s %s - %s sr:%f n:%d d:%s" % \
                    (i,stats.station, stats.channel, stats.starttime, stats.endtime, \
                     stats.sampling_rate, stats.npts, tr.data.dtype))
        '''

        resulted_stream.trim(UTCDateTime(start_time / 1e9), UTCDateTime(end_time / 1e9), pad=True, fill_value=0.0)

        final_output = construct_output(resulted_stream, output_format)

        request_endtime = time.time() - request_starttime
        log.info("Request Done Successfully. Total API Request took: %.2f seconds" % request_endtime)

        return send_file(final_output, attachment_filename="testing.mseed", as_attachment=True)
    else:
        request_endtime = time.time() - request_starttime
        log.info("Request Done Successfully but with no data found. Total API Request took: %.2f seconds" % request_endtime)

        return build_success_response("No data found", {"no_data_found": True})



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

    #log.info("Filter: %s" % filter)
    return filter


def generate_filename_from_date(dt):
    return datetime.datetime.utcfromtimestamp(dt / 1e9).strftime("%Y%m%d_%H%M%S%f")


def generate_filepath_from_date(dt):
    return datetime.datetime.utcfromtimestamp(dt / 1e9).strftime("/%Y/%m/%d/%H/")


# def create_filestore_directories(basedir, filepath):
#     events_dir = basedir + "events" + filepath
#     waveforms_dir = basedir + "waveforms" + filepath
#
#     # create directory in events if not exists
#     if not os.path.exists(events_dir):
#         os.makedirs(events_dir)
#     # create directory in events if not exists
#     if not os.path.exists(waveforms_dir):
#         os.makedirs(waveforms_dir)


# def construct_relative_files_paths(filepath, filename):
#     # events
#     event_filepath = "events" + filepath + filename + ".xml"
#     waveform_filepath = "waveforms" + filepath + filename + ".mseed"
#     waveform_context_filepath= "waveforms" + filepath + filename + ".mseed_context"
#     return event_filepath, waveform_filepath, waveform_context_filepath


def create_filestore_directories(basedir, filepath):
    store_dir = basedir + filepath

    # create directory if not exists
    if not os.path.exists(store_dir):
        os.makedirs(store_dir)


def construct_relative_files_paths(filepath, filename):
    # events
    event_filepath = filepath + filename + "_v000.xml"
    waveform_filepath = filepath + filename + "_v000.mseed"
    waveform_context_filepath= filepath + filename + "_v000.mseed_context"
    return event_filepath, waveform_filepath, waveform_context_filepath


def check_event_existance(event_time_epoch):
    filter = {
        'time': {
            '$gte': event_time_epoch - 1e8,
            '$lte': event_time_epoch + 1e8
        }
    }

    result = mongo.db[EVENTS_COLLECTION].find(filter)

    if result.count() >= 1:
        return str(result[0]["_id"])
    else:
        return None


@app.route('/events/putEvent', methods=['POST'])
def put_event():

    log.info("put_event started.")
    request_starttime = time.time()

    if 'event' in request.get_json() and 'waveform' in request.get_json() and 'context' in request.get_json():
        conversion_starttime = time.time()
        log.info('put_event: call read_events')
        event = read_events(BytesIO(base64.b64decode(request.get_json()['event'])))[0]
        waveform = read(BytesIO(base64.b64decode(request.get_json()['waveform'])), format='MSEED')
        waveform_context = read(BytesIO(base64.b64decode(request.get_json()['context'])), format='MSEED')
        conversion_endtime = time.time() - conversion_starttime
        log.info("put_event: Conversion took: %.2f seconds" % conversion_endtime)
    else:
        raise InvalidUsage("Wrong data sent..!! Event, Waveform and Context must be specified in request body",
                           status_code=400)

    log.info('put_event: Call EventDB.flatten_event')
    ev_flat_dict = EventDB.flatten_event(Event(event))

    existed_event_id = check_event_existance(ev_flat_dict['time'])

    if existed_event_id:
        #return build_success_response("Event already exists with ID " + existed_event_id)
        log.info('put_event: id=[%s] already exists in db --> Dont try to insert/update xml' % existed_event_id)
        return build_success_response("Event already exists with ID " + existed_event_id, {"event_id": str(existed_event_id)})
    else:
        filename = generate_filename_from_date(ev_flat_dict['time'])
        filepath = generate_filepath_from_date(ev_flat_dict['time'])
        log.info('put_event: create filestore: filepath + filename = [%s]' % (filepath + filename))

        create_filestore_directories(BASE_DIR, filepath)

        event_filepath, waveform_filepath, waveform_context_filepath = construct_relative_files_paths(filepath, filename)

        ev_flat_dict['filename'] = filename
        ev_flat_dict['event_filepath'] = event_filepath
        ev_flat_dict['waveform_filepath'] = waveform_filepath
        ev_flat_dict['waveform_context_filepath'] = waveform_context_filepath
        ev_flat_dict['insertion_time'] = int(datetime.datetime.utcnow().timestamp() * 1e9)
        ev_flat_dict['modification_time'] = None

        # write files in filestore
        event.write(BASE_DIR + event_filepath, format="QUAKEML")
        waveform.write(BASE_DIR + waveform_filepath, format="MSEED")
        waveform_context.write(BASE_DIR + waveform_context_filepath, format="MSEED")

        inserted_event_id = mongo.db[EVENTS_COLLECTION].insert_one(ev_flat_dict).inserted_id

        log.info("put_event: Inserted new event into mongoDB with ID:" + str(inserted_event_id))

        request_endtime = time.time() - request_starttime
        log.info("put_event ended Successfully. Total API Request took: %.2f seconds" % request_endtime)

        return build_success_response("Event inserted successfully", {"event_id": str(inserted_event_id)})


def read_and_write_file_as_bytes(relative_filepath, format):
    bytes = BytesIO()
    obj = None
    # construct full filepath
    full_filepath = BASE_DIR + relative_filepath

    if format == 'QUAKEML':
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
    elif requested_format == "ALL":
        tar_file_bytes = BytesIO()
        #with zipfile.ZipFile(zip_file_bytes, 'w') as myzip:
        with tarfile.open(fileobj=tar_file_bytes, mode='w:gz') as mytar:
            for out_type in OUTPUT_TYPES.keys():
                if out_type != 'ALL':
                    file = construct_event_output(event_data, out_type)
                    fname = event_data['filename'] + OUTPUT_TYPES[out_type]
                    info = tarfile.TarInfo(fname)
                    info.size = file.getbuffer().nbytes
                    mytar.addfile(info, file)
        mytar.close()
        output = tar_file_bytes

    output_size = sys.getsizeof(output.getvalue())
    output.seek(0)
    end_time = time.time() - start_time
    log.info("Formatting Output took: %.2f , Output Size: %.2f MB" % (end_time, (output_size/1024/1024)))
    return output


@app.route('/events/getEvent', methods=['GET'])
def get_event():

    log.info("getEvent started")
    request_starttime = time.time()

    # Check Date Ranges
    if 'eventid' in request.args:
        log.info('get_Event: eventid:%s' % request.args['eventid'])
        event_id = request.args['eventid']
    else:
        raise InvalidUsage("Event ID must be specified like:" +
                           "eventid=<ID>",
                           status_code=400)

    if 'output' in request.args:
        if request.args['output'] in OUTPUT_TYPES.keys():
            output_format = request.args['output']
        else:
            raise InvalidUsage("Invalid output option, should be one of these:" +
                               "MSEED, MSEED_CONTEXT, QUAKEML or ALL",
                               status_code=400)
    else:
        output_format = "ALL"

    query_filter = {"_id": ObjectId(event_id)}

    event_result = mongo.db[EVENTS_COLLECTION].find(query_filter, {"_id": 0})

    if event_result.count() == 1:

        output_file = construct_event_output(event_result[0], output_format)

        # construct downloaded filename
        output_filename = event_result[0]['filename'] + OUTPUT_TYPES[output_format]

        request_endtime = time.time() - request_starttime
        log.info("getEvent ended Successfully. Total API Request took: %.2f seconds" % request_endtime)

        return send_file(output_file, attachment_filename=output_filename, as_attachment=True)

    else:
        log.info("getEvent ended Successfully with no data found")
        return build_success_response("No data found")


@app.route('/events/updateEvent', methods=['POST'])
def update_event():

    log.info("update_event started")

    request_starttime = time.time()
    event = None
    waveform = None
    waveform_context = None

    if 'event_id' in request.get_json() and ('event' in request.get_json()
                                             or 'waveform' in request.get_json()
                                             or 'context' in request.get_json()):
        conversion_starttime = time.time()

        event_id = request.get_json()['event_id']
        log.info("update_event: request to update event_id:%s" % event_id)
        if 'event' in request.get_json():
            event = read_events(BytesIO(base64.b64decode(request.get_json()['event'])))[0]
        if 'waveform' in request.get_json():
            waveform = read(BytesIO(base64.b64decode(request.get_json()['waveform'])), format='MSEED')
        if 'context' in request.get_json():
            waveform_context = read(BytesIO(base64.b64decode(request.get_json()['context'])), format='MSEED')
        conversion_endtime = time.time() - conversion_starttime

        log.info("Conversion took: %.2f seconds" % conversion_endtime)
    else:
        raise InvalidUsage("Wrong data sent..!! Event ID must be specified and one of " +
                           "(Event, Waveform or Context) must be specified in request body",
                           status_code=400)

    filter = {
        '_id': ObjectId(event_id),
    }

    event_document = mongo.db[EVENTS_COLLECTION].find_one(filter)

    if event_document:

        log.info('update_event: Found event_document event_id:%s' % event_id)

        filename = generate_filename_from_date(event_document['time'])
        filepath = generate_filepath_from_date(event_document['time'])

        log.info('update_event: create_filestore: BASE_DIR=%s filepath=%s' % (BASE_DIR, filepath))

        create_filestore_directories(BASE_DIR, filepath)

        event_document['filename'] = filename

        # update files in filestore
        if event:
            log.info('put_event: Call EventDB.flatten_event')
            # read new event as dictionary
            ev_flat_dict = EventDB.flatten_event(Event(event))
            # update event db object with the new data in QUAKEML file
            for key in ev_flat_dict.keys():
                event_document[key] = ev_flat_dict[key]
            event_filepath = update_file_version(event_document['event_filepath'])
            event_document['event_filepath'] = event_filepath
            event.write(BASE_DIR + event_filepath, format="QUAKEML")
            log.info('update_event: event_filepath=%s' % (event_filepath))

        if waveform:
            waveform_filepath = update_file_version(event_document['waveform_filepath'])
            event_document['waveform_filepath'] = waveform_filepath
            waveform.write(BASE_DIR + waveform_filepath, format="MSEED")
            log.info('update_event: waveform_filepath=%s' % (waveform_filepath))

        if waveform_context:
            waveform_context_filepath = update_file_version(event_document['waveform_context_filepath'])
            event_document['waveform_context_filepath'] = waveform_context_filepath
            waveform_context.write(BASE_DIR + waveform_context_filepath, format="MSEED")
            log.info('update_event: waveform_context_filepath=%s' % (waveform_context_filepath))


        event_document['modification_time'] = int(datetime.datetime.utcnow().timestamp() * 1e9)

        mongo.db[EVENTS_COLLECTION].update(filter, event_document, upsert=False)

        log.info("Event  with ID:%s has been updated successfully" % event_id)

        request_endtime = time.time() - request_starttime
        log.info("updateEvent ended Successfully. Total Update Request took: %.2f seconds" % request_endtime)

        #return build_success_response("Event updated successfully")
        return build_success_response("Event update successfully", {"event_id": str(event_id)})
    else:
        log.info("updateEvent ended Successfully. No data found to update.")
        return build_success_response("No data found")


def update_file_version(filename):
    regex_formula = r"\_v\d{3}\."
    regex = re.compile(regex_formula)
    str_matched_list = regex.findall(filename)
    new_version_str = ""
    if str_matched_list:
        current_version = str_matched_list[0][2:-1]
        log.info(current_version)
        n_version = int(current_version) + 1
        new_version_str = "_v" + str(n_version).zfill(3) + "."
        filename = regex.sub(new_version_str, filename)
    else:
        filename = filename.replace(".", "_v001.")
    return filename


@app.route('/events/getEventInUse', methods=['GET'])
def get_event_inuse():

    log.info("getEventInUse started")
    request_starttime = time.time()

    # Check Date Ranges
    if 'eventid' and 'userid' in request.args:
        print('get_Event_InUse: eventid:%s userid:%s' % (request.args['eventid'], request.args['userid']))
        event_id = request.args['eventid']
        user_id = request.args['userid']
    else:
        raise InvalidUsage("Event ID and User ID must be specified like:" +
                           "(eventid=<ID>) & (userid=<ID>)",
                           status_code=400)

    # get current timestamp in order to fetch inuse events till now
    current_timestamp = time.time() * 10e9

    query_filter = {
        "event_id": ObjectId(event_id),
        "user_id": ObjectId(user_id),
        "ttl_expiration": {'$gte': current_timestamp}
        }

    inuse_result = mongo.db[EVENTS_INUSE_COLLECTION].find(query_filter)

    request_endtime = time.time() - request_starttime
    log.info("getEventInUse ended Successfully. Total API Request took: %.2f seconds" % request_endtime)

    if inuse_result.count() >= 1:
        return MongoJSONEncoder().encode(inuse_result[0])
    else:
        return build_success_response("No data found")


@app.route('/events/putEventInUse', methods=['POST'])
def put_event_inuse():

    log.info("putEventInUse started")
    request_starttime = time.time()

    # Check Date Ranges
    if 'eventid' and 'userid' in request.get_json():
        log.info('put_Event_InUse: eventid:%s userid:%s' % (request.get_json()['eventid'], request.get_json()['userid']))
        event_id = request.get_json()['eventid']
        user_id = request.get_json()['userid']
    else:
        raise InvalidUsage("Event ID and User ID must be specified in request body",
                           status_code=400)

    # get current timestamp in order to calculate inuse TTL expiration time
    ttl_expiration = (time.time() + INUSE_TTL) * 10e9

    inuse_data = {
        "event_id": ObjectId(event_id),
        "user_id": ObjectId(user_id),
        "ttl_expiration": ttl_expiration
        }

    inserted_record_id = mongo.db[EVENTS_INUSE_COLLECTION].insert_one(inuse_data).inserted_id

    request_endtime = time.time() - request_starttime
    log.info("putEventInUse ended Successfully. Total API Request took: %.2f seconds" % request_endtime)

    return build_success_response("EventInUse added successfully", {"event_inuse_id": str(inserted_record_id)})


def build_success_response(message_text, extra_dict=None):
    success = True
    response = {
        'success': success,
        'message': message_text
    }
    if extra_dict:
        response.update(extra_dict)
    return json.dumps(response)


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


@app.errorhandler(Exception)
def handle_error(error):
    message = [str(x) for x in error.args]
    status_code = 500
    success = False
    response = {
        'success': success,
        'error': {
            'type': error.__class__.__name__,
            'message': message
        }
    }
    log.exception(response)
    return json.dumps(response), status_code

if __name__ == '__main__':
    # launch API
    log.info("API is starting")
    #app.run(debug=True)
    app.run(host='0.0.0.0', threaded=True)

