import yaml
import os


class Configuration:

    IMS_CONFIG = None
    DB_CONFIG = None

    def __init__(self):
        config_dir = os.environ['SPP_CONFIG']
        ims_fname = os.path.join(config_dir, 'data_connector_config.yaml')

        with open(ims_fname, 'r') as cfg_file:
            params = yaml.load(cfg_file)
            self.IMS_CONFIG = params['data_connector']

        db_config_file = os.path.join(config_dir, 'permanent_db.yaml')

        with open(db_config_file, 'r') as cfg_file:
            params = yaml.load(cfg_file)
            self.DB_CONFIG = params['db']