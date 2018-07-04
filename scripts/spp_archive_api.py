from flask import Flask, request
import yaml
import os
import sys
from microquake.db.mongo.mongo import MongoDBHandler, StreamDB
from microquake.core.stream import Stream
from microquake.core import read
from io import BytesIO
from microquake.core.util import serializer


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
    for encoded_tr in db_result:
        bstream = serializer.decode_base64(encoded_tr['encoded_mseed'])
        tr = read(BytesIO(bstream))[0]
        traces.append(tr)

    stout = Stream(traces=traces)
    buf = BytesIO()
    stout.write(buf, format=requested_format)
    return buf


@app.route('/getAll', methods=['GET'])
def get_all_streams():

    # Query traces from mongo collection
    result = mongo.db.traces.find({})
    # combine traces together
    stream_data_buffer = combine_stream_data(result)
    # compress and encode the result
    response_data = serializer.encode_base64(stream_data_buffer)
    print("=======> Request Done and Size of returned data is:", sys.getsizeof(response_data))
    return response_data


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The URL is invalid, please check the URL requested</p>", 404


if __name__ == '__main__':

    # launch API
    app.run(debug=True)