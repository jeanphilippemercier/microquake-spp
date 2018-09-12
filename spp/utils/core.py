def get_stations():
    import os
    from microquake.core import read_stations
    common = os.environ['SPP_COMMON']
    station_file = os.path.join(common, 'sensors.csv')

    return read_stations(station_file, format='CSV', has_header=True)


def get_nll_dir():
    from microquake.core.ctl import parse_control_file
    import os
    from IPython.core.debugger import Tracer
    common = os.environ['SPP_CONFIG']
    nll_config_file = os.path.join(common, 'project.xml')
    params = parse_control_file(nll_config_file)
    return params.nll.NLL_BASE


def parse_nll_control_file():
    import os
    from microquake.core.ctl import parse_control_file
    config = os.environ['SPP_CONFIG']
    nll_config = os.path.join(config, 'microquake.xml')
    params = parse_control_file(nll_config)

    return params


def get_data_connector_parameters():

    import os
    import yaml
    from dateutil import parser
    from spp.time import get_time_zone

    config_dir = os.environ['SPP_CONFIG']

    fname = os.path.join(config_dir, 'data_connector_config.yaml')

    tz = get_time_zone()

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['data_connector']

    if 'starttime' in params.keys():
        params['starttime'] = parser.parse(params['starttime'])
        params['starttime'].replace(tzinfo=tz)

    if 'endtime' in params.keys():
        params['endtime'] = parser.parse(params['endtime'])
        params['endtime'].replace(tzinfo=tz)

    return params


def get_project_params():

    import os
    from microquake.core import ctl

    config_dir = os.environ['SPP_CONFIG']

    return ctl.parse_control_file(config_dir + '/project.xml')


def encode_avro(trace_json):
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
    writer.write(trace_json, encoder)

    return bytes_writer.getvalue()


def decode_avro(avro):
    """
    this function needs to be completed
    :param avro:
    :return:
    """

    stream = none

    return stream

