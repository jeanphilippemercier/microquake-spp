from spp.ims_connector import core
from importlib import reload

reload(core)


if __name__ == "__main__":

    # read yaml file

    import os
    import yaml
    from microquake.core.event import Catalog
    import numpy as np

    config_dir = os.environ['SPP_CONFIG']
    common_dir = os.environ['SPP_COMMON']

    fname = os.path.join(config_dir, 'ims_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['ims_connector']

    if params['data_source']['type'] == 'remote':
        for st in core.request_handler():
            print(st)

            # write to Kafka

    elif params['data_source']['type'] == 'local':
        location = params['data_source']['location']
        period = params['period']
        window_length = params['window_length']

        for i in np.arange(0, period, window_length):
            st = core.request_handler_local(location)
            print(st)

        # write to Kafka