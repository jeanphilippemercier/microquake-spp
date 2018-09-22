from microquake.core import read
from io import BytesIO
from base64 import b64decode, b64encode
import sys
import time
import json
import pickle
from spp.utils import core
import os
#from avro.io import DatumWriter
import avro.io
import avro.schema
import io
import ast
import fastavro
import uuid


config_dir = os.environ['SPP_CONFIG']
schema_path = config_dir + '/mseed_avro_schema.avsc'
schema_favro_path = config_dir + '/mseed_avro_schema.avsc'
schema_txt = open(schema_path).read()
schema_favro_txt = ast.literal_eval(open(schema_favro_path).read())
schema = avro.schema.Parse(schema_txt)

schema_favro = fastavro.parse_schema(schema_favro_txt)


def encode_avro(trace_json):
    """
    This function needs to be completed
    :param stream:
    :return:
    """

    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(trace_json, encoder)

    return bytes_writer.getvalue()


def encode_fastavro(json_object):

    converted_obj = None
    tmp_dir = ""
    tmp_file_name = tmp_dir + "trace_tmp_" + uuid.uuid4().hex + ".avro"
    with open(tmp_file_name, 'wb') as out:
        fastavro.schemaless_writer(out, schema_favro, json_object)

    with open(tmp_file_name, mode='rb') as file:
        converted_obj = file.read()

    os.remove(tmp_file_name)
    return converted_obj


def test_serializer(stream, type):

    msg_size = (sys.getsizeof(stream) / 1024 / 1024)
    print("Stream Size:", "%.2f" % msg_size, "MB")

    stime = time.time()
    traces_list = stream.to_traces_json()
    etime = time.time() - stime
    print("==> converted stream object to json in:", "%.2f" % etime)
    msg_size = (sys.getsizeof(traces_list) / 1024 / 1024)
    print("Converted Size:", "%.2f" % msg_size, "MB")

    total_ser_size = 0

    stime = time.time()



    for trace in traces_list:
        #print("Serializing Trace Started")
        ser_stime = time.time()
        if type == "json":
            msg = json.dumps(trace).encode('utf-8')
        elif type == "avro":
            msg = encode_avro(trace)
        elif type == "pickle":
            msg = pickle.dumps(trace)
        elif type == "fastavro":
            msg = encode_fastavro(trace)

        ser_time = time.time() - ser_stime
        total_ser_size += sys.getsizeof(msg)

    etime = time.time() - stime

    print("==> Total Time:", "%.2f" % etime,
          ", Traces Count:", len(traces_list),
          ", Avg Time:", "%.2f" % (etime/len(traces_list)),
          ", Avg Size:", "%.2f" % (total_ser_size / len(traces_list) /1024/1024))

    print("==================================================================")


if __name__ == "__main__":
    # reading the data
    stream = read('/Users/hanee/Rio_Tinto/sample_data/20180523_190001_900000_20180523_190003_100066.mseed',
                  format='MSEED')

    print("Starting...")
    test_serializer(stream, "json")
    #test_serializer(stream, "avro")
    test_serializer(stream, "pickle")
    test_serializer(stream, "fastavro")
