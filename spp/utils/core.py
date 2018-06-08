def get_stations():
    import os
    from microquake.core import read_stations
    common = os.environ['SPP_COMMON']
    station_file = os.path.join(common, 'sensors.csv')

    return read_stations(station_file, format='CSV', has_header=True)


def encode_avro(stream):
    """
    This function needs to be completed
    :param stream:
    :return:
    """
    import os

    config_dir = os.environ['SPP_CONFIG']

    avro = None

    return avro


def decode_avro(avro):
    """
    this function needs to be completed
    :param avro:
    :return:
    """

    stream = none

    return stream

