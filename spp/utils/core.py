def get_stations():
    import os
    from microquake.core import read_stations
    common = os.environ['SPP_COMMON']
    station_file = os.path.join(common, 'sensors.csv')

    return read_stations(station_file, format='CSV', has_header=True)


def get_nll_dir():
    from microquake.core.ctl import parse_control_file
    params = parse_control_file(nll_config)






    params = parse_control_file()


def parse_nll_control_file():
    import os
    from microquake.core.ctl import parse_control_file
    common = os.environ['SPP_COMMON']
    config = os.environ['SPP_CONFIG']

    nll_config = os.path.join(config, 'microquake.xml')

    params = parse_control_file(nll_config)


def get_data_connector_parameters():

    import os
    import yaml

    config_dir = os.environ['SPP_CONFIG']

    fname = os.path.join(config_dir, 'data_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['ims_connector']

    return params




def encode_avro(stream):
    """
    This function needs to be completed
    :param stream:
    :return:
    """
    import os
    #from avro.io import DatumWriter
    import avro.io
    import avro.schema
    import io

    config_dir = os.environ['SPP_CONFIG']
    schema_path = config_dir + '/mseed_avro_schema.avsc'
    schema = avro.schema.Parse(open(schema_path).read())

    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(stream, encoder)

    return bytes_writer.getvalue()


def decode_avro(avro):
    """
    this function needs to be completed
    :param avro:
    :return:
    """

    stream = none

    return stream

