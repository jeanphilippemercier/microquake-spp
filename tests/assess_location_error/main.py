from microquake.core import read_events

from datetime import datetime, timedelta
from spp.time import get_time_zone
from microquake.IMS import web_api
from spp.utils import get_stations, get_project_params
from microquake import nlloc
from IPython.core.debugger import Tracer


tz = get_time_zone()
blast_times = [datetime(2018, 6, 25, 18, 58, 9, tzinfo=tz),
               datetime(2018, 6, 15, 18, 46, 15, tzinfo=tz),
               datetime(2018, 6, 13, 7, 2, 33, tzinfo=tz),
               datetime(2018, 6, 13, 7, 2, 35, tzinfo=tz),
               datetime(2018, 6, 7, 6, 59, 59, tzinfo=tz),
               datetime(2018, 6, 6, 18, 51, 42, tzinfo=tz),
               datetime(2018, 6, 2, 19, 7, 1, tzinfo=tz),
               datetime(2018, 5, 30, 6, 48, 19, tzinfo=tz),
               datetime(2018, 5, 26, 6, 53, 39, tzinfo=tz),
               datetime(2018, 5, 24, 19, 5, 40, tzinfo=tz)]

params = get_project_params()

base_url = "http://10.95.74.35:8002/ims-database-server/databases/mgl"
#
# # cat = web_api.get_catalogue(base_url, starttime, endtime, site, blast=False,
# # 			    get_arrivals=True)

site = get_stations()
params = get_project_params()

nll_opts = nlloc.init_nlloc_from_params(params)

# nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)

cats = []
for bt in blast_times:
    starttime = bt - timedelta(seconds=1)
    endtime = bt + timedelta(seconds=1)

    cat_tmp = web_api.get_catalogue(base_url, starttime, endtime, site,
                                blast=True, event=False, get_arrivals=True)

    for evt in cat_tmp:
        print('locating event')
        cat_out = nll_opts.run_event(evt)
        cats.append(cat_out)
        print(cat_out[0].preferred_origin())
        # Tracer()()



