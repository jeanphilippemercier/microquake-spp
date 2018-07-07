from flask import Flask, request, jsonify
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


app = Flask(__name__)

# load configuration file
config_dir = os.environ['SPP_CONFIG']
config_file = os.path.join(config_dir, 'permanent_db.yaml')
with open(config_file, 'r') as cfg_file:
    params = yaml.load(cfg_file)
    db_params = params['db']

mongo = MongoDBHandler(db_params['uri'], db_params['db_name'])


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Ingestion MSEED Data Retrieval</h1>
<p></p>'''


def combine_stream_data(db_result, requested_format='MSEED'):
    traces = []
    start_time = time.time()
    for encoded_tr in db_result:
        bstream = serializer.decode_base64(encoded_tr['encoded_mseed'])
        tr = read(BytesIO(bstream))[0]
        traces.append(tr)

    end_time = time.time() - start_time
    print("==> DB Fetching took: ", "%.2f" % end_time, "Records Count:", len(traces))

    stout = Stream(traces=traces)
    buf = BytesIO()
    stout.write(buf, format=requested_format)
    return buf


def check_and_parse_datetime(dt_str):
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f")
        return dt
    except ValueError:
        error_msg = "Invalid datetime format, value should be in format: %Y-%m-%dT%H:%M:%S.%f"
        print(error_msg)
        raise InvalidUsage(error_msg, status_code=410)


@app.route('/getStream', methods=['GET'])
def get_stream():

    request_starttime = time.time()

    # Validate Request Params
    # Check Date Ranges
    if 'starttime' in request.args and ('endtime' in request.args or 'duration' in request.args):
        print(request.args['starttime'])
        start_time = check_and_parse_datetime(request.args['starttime'])

        if 'endtime' in request.args:
            end_time = check_and_parse_datetime(request.args['endtime'])
        else:
            end_time = start_time + timedelta(0, int(request.args['duration']))

    else:
        raise InvalidUsage("date-range-options must be specified like:" +
                           "(starttime=<time>) & ([endtime=<time>] | [duration=<seconds>])",
                           status_code=411)

    # channel-options      ::  (net=<network> & sta=<station> cha=<channel>)
    if 'net' in request.args:
        network = request.args['net']
    else:
        raise InvalidUsage("network option must be specified like:" +
                           "net=<network>",
                           status_code=411)

    if 'sta' in request.args:
        station = request.args['sta']
    else:
        raise InvalidUsage("station option must be specified like:" +
                           "sta=<station>",
                           status_code=411)

    if 'cha' in request.args:
        channel = request.args['cha']
    else:
        raise InvalidUsage("channel option must be specified like:" +
                           "cha=<channel>",
                           status_code=411)

    criteria_filter = construct_filter_criteria(start_time, end_time, network, station, channel)

    result = mongo.db.traces.find(criteria_filter)
    # combine traces together
    stream_data_buffer = combine_stream_data(result)
    # compress and encode the result
    response_data = serializer.encode_base64(stream_data_buffer)

    request_endtime = time.time() - request_starttime
    print("=======> Request Done and Size of returned data is:",
          "%.2f" % (sys.getsizeof(response_data)/1024/1024), "MB",
          "Request took: ", "%.2f" % request_endtime, "seconds")

    return response_data


def construct_filter_criteria(start_time, end_time, network, station, channel):

    #start_time = int(np.float64(UTCDateTime(start_time).timestamp) * 1e9)  #UTCDateTime(start_time).timestamp
    #print("---", start_time)
    #end_time = int(np.float64(UTCDateTime(end_time).timestamp) * 1e9)

    filter = {
        'stats.starttime': {
            '$gte': start_time,
            '$lt': end_time
         },
        'stats.network': network
    }

    if station != 'ALL':
        filter['stats.station'] = station

    if channel != 'ALL':
        filter['stats.channel'] = channel

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