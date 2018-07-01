from microquake.IMS import web_api
from microquake.core import read_stations
from spp import time
from datetime import datetime
import yaml
from microquake.core import ctl
from microquake import nlloc
import os

tz = time.get_time_zone()

starttime = datetime(2018, 4, 15, 3, 40, tzinfo=tz)
endtime = datetime(2018, 4, 15, 3, 50, tzinfo=tz)

config_dir = os.environ['SPP_CONFIG']
common_dir = os.environ['SPP_COMMON']
fname = os.path.join(config_dir, 'ingest_config.yaml')
config_file = os.path.join(config_dir, 'project.xml')

with open(fname, 'r') as cfg_file:
    params = yaml.load(cfg_file)
    params = params['data_ingestion']


base_url = params['data_source']['location']

sensor_file = os.path.join(common_dir, 'sensors.csv')
site = read_stations(sensor_file, has_header=True)


cat = web_api.get_catalogue(base_url, starttime, endtime, site, blast=False,
                            event=True, accepted=True, manual=True,
                            get_arrivals=True)

# st = web_api.get_seismogram_event(base_url, cat[0], 'OT')


params = ctl.parse_control_file(config_file)
nll_opts = nlloc.init_nlloc_from_params(params)

# The following line only needs to be run once. It creates the base directory
nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)


event_loc = nll_opts.run_event(cat[0])




