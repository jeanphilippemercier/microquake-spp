from microquake.core import ctl
import os

from microquake import nlloc

from spp.time import get_time_zone
from microquake.IMS import web_api
from datetime import datetime, timedelta
from microquake.core import read_stations, read_events
from microquake.core.event import Catalog

config_dir = os.environ['SPP_CONFIG']
config_file = config_dir + '/input.xml'

common_dir = os.environ['SPP_COMMON']
sensor_file = common_dir + '/sensors.csv'

site = read_stations(sensor_file, has_header=True)

params = ctl.parse_control_file(config_file)

nll_opts = nlloc.init_nlloc_from_params(params)


# The following line only needs to be run once. It creates the base directory
# nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)

tz = get_time_zone()

endtime = datetime.now().replace(tzinfo=tz)
starttime = endtime - timedelta(days=3)
base_url = "http://10.95.64.12:8002/ims-database-server/databases/mgl"

# cat = web_api.get_catalogue(base_url, starttime, endtime, site, blast=False, 
# 			    get_arrivals=True)
# cat.write('test.xml', format='QUAKEML')

cat = read_events('test.xml', format='QUAKEML')

evts_out = []
for event in cat:
    evts_out.append(nll_opts.run_event(event)[0])
    print(evts_out[-1])
    

cat_loc = Catalog(events=evts_out)

