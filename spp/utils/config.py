import yaml
import os


class Configuration:

    DATA_CONNECTOR = None
    DB = None

    def __init__(self):
        config_dir = os.environ['SPP_CONFIG']
        data_conn_fname = os.path.join(config_dir, 'data_connector_config.yaml')

        with open(data_conn_fname, 'r') as cfg_file:
            params = yaml.load(cfg_file)
            self.DATA_CONNECTOR = params['data_connector']

        # db_config_file = os.path.join(config_dir, 'permanent_db.yaml')
        #
        # with open(db_config_file, 'r') as cfg_file:
        #     params = yaml.load(cfg_file)
        #     self.DB = params['db']


#CONFIG = Configuration()