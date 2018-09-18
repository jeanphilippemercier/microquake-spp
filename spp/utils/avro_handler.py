import os
import avro.schema
import io
import uuid
import fastavro
import ast


def parse_avro_schema(schema_filename):
    config_dir = os.environ['SPP_CONFIG']
    schema_path = config_dir + '/' + schema_filename
    return avro.schema.Parse(open(schema_path).read())


def parse_fastavro_schema(schema_filename):
    config_dir = os.environ['SPP_CONFIG']
    schema_path = config_dir + '/' + schema_filename
    schema_favro_txt = ast.literal_eval(open(schema_path).read())
    return fastavro.parse_schema(schema_favro_txt)


def encode_avro(schema, trace_json):
    """
    This function needs to be completed
    :param stream:
    :return:
    """

    # config_dir = os.environ['SPP_CONFIG']
    # schema_path = config_dir + '/mseed_avro_schema.avsc'
    # schema = avro.schema.Parse(open(schema_path).read())

    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(trace_json, encoder)

    return bytes_writer.getvalue()


def decode_avro(avro):
    """
    this function needs to be completed
    :param avro:
    :return:
    """
    stream = None
    return stream


def encode_fastavro(schema, json_object):
    converted_obj = None
    tmp_dir = "" # to be added as config
    tmp_file_name = tmp_dir + "trace_tmp_" + uuid.uuid4().hex + ".avro"
    with open(tmp_file_name, 'wb') as out:
        fastavro.schemaless_writer(out, schema, json_object)

    with open(tmp_file_name, mode='rb') as file:
        converted_obj = file.read()

    os.remove(tmp_file_name)
    return converted_obj


def decode_fastavro(schema, avro_object):
    tmp_dir = ""  # to be added as config
    tmp_file_name = tmp_dir + "trace_tmp_" + uuid.uuid4().hex + ".avro"
    with open(tmp_file_name, 'wb') as f:
       f.write(avro_object)

    with open(tmp_file_name, 'rb') as fp:
        json_object = fastavro.schemaless_reader(fp, schema)

    os.remove(tmp_file_name)
    return json_object