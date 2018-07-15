from flask import Flask, request, jsonify, send_file
import yaml
import os
import sys
from microquake.db.mongo.mongo import MongoDBHandler, StreamDB
from microquake.core.stream import Stream
from microquake.core import read
from io import BytesIO
from microquake.core.util import serializer
from datetime import datetime, timedelta
import time
import json
from obspy.core.utcdatetime import UTCDateTime
import numpy as np
from base64 import b64decode,b64encode
from microquake.core.trace import Trace
from obspy.core.util.attribdict import AttribDict


app = Flask(__name__)

# load configuration file
config_dir = os.environ['SPP_CONFIG']
config_file = os.path.join(config_dir, 'permanent_db.yaml')
with open(config_file, 'r') as cfg_file:
    params = yaml.load(cfg_file)
    db_params = params['db']

mongo = MongoDBHandler(db_params['uri'], db_params['db_name'])
COLLECTION = "traces_json"


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

    # compress and encode the result
    ### use for compressed
    # response_data = serializer.encode_base64(stream_data_buffer)
    ### use for uncompressed
    # response_data = b64encode(stream_data_buffer.getvalue())

    end_time = time.time() - start_time
    print("==> Formatting Output took: ", "%.2f" % end_time, "Output Size:", "%.2f" % (output_size/1024/1024), "MB")
    return output


def combine_json_to_stream_data(db_result):
    start_time = time.time()
    s = Stream.create_from_json_traces(db_result).merge(method=0)
    end_time = time.time() - start_time
    print("==> DB Fetching took: ", "%.2f" % end_time, "Records Count:", len(s.traces))
    return s


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

        print(request.args['starttime'])
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

    result = mongo.db[COLLECTION].find(criteria_filter, {"_id": 0})

    # combine traces together
    resulted_stream = combine_json_to_stream_data(result)

    final_output = construct_output(resulted_stream, output_format)

    request_endtime = time.time() - request_starttime
    print("=======> Request Done Successfully.",
          "Total API Request took: ", "%.2f" % request_endtime, "seconds")

    return send_file(final_output, attachment_filename="testing.mseed", as_attachment=True)


def construct_filter_criteria(start_time, end_time, network, station, channel):

    filter = {
        'stats.starttime': {
            '$gte': start_time,
            '$lt': end_time
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
    app.run(debug=True)
    #app.run(threaded=True)