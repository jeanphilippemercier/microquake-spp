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