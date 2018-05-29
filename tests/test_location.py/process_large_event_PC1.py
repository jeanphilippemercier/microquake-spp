from microquake.IMS import web_api
from microquake.core import read_stations, read_events, read
from spp import time
from datetime import datetime, timedelta
import yaml
from microquake.core import ctl
from microquake import nlloc
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

tz = time.get_time_zone()

starttime = datetime(2018, 4, 15, 3, 40, tzinfo=tz)
endtime = datetime(2018, 4, 15, 3, 50, tzinfo=tz)

config_dir = os.environ['SPP_CONFIG']
common_dir = os.environ['SPP_COMMON']
fname = os.path.join(config_dir, 'ingest_config.yaml')
config_file = os.path.join(config_dir, 'input.xml')

with open(fname, 'r') as cfg_file:
    params = yaml.load(cfg_file)
    params = params['data_ingestion']


base_url = params['data_source']['location']

sensor_file = os.path.join(common_dir, 'sensors.csv')
site = read_stations(sensor_file, has_header=True)


# cat = web_api.get_catalogue(base_url, starttime, endtime, site, blast=False,
#                             event=True, accepted=True, manual=True,
#                             get_arrivals=True)
# #
# # # st = web_api.get_seismogram_event(base_url, cat[0], 'OT')
# #
# #
# params = ctl.parse_control_file(config_file)
# nll_opts = nlloc.init_nlloc_from_params(params)
# #
# # # The following line only needs to be run once. It creates the base directory
# # nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)
# #
# #
# event_loc = nll_opts.run_event(cat[0])
# event_loc.write('../../data/2018-04-15_034422.xml', format='QuakeML')

event_loc = read_events('../../data/2018-04-15_034422.xml')
st = read('../../data/2018-04-15_034422.mseed')

res_ims = []
res_ims = [arrival.time_residual for arrival in event_loc[0].origins[
    0].arrivals]

res_nll = [arrival.time_residual for arrival in event_loc[0].origins[
    1].arrivals]

plt.figure(1)
plt.clf()

dists = [arrival.distance for arrival in event_loc[0].origins[1].arrivals]
stations = [arrival.pick.waveform_id.station_code for arrival in event_loc[
    0].origins[1].arrivals]

indices = np.argsort(dists)
stations = np.array(stations)[indices]
dists = np.array(dists)[indices]

st_comp = st.composite()

arrivals = event_loc[0].origins[1].arrivals
#arrivals2 = event_loc[0].origins[0].arrivals

stations = []
scale = 10
times = []
distances = []

plt.figure(1)
plt.clf()
ax = plt.subplot()

for k, arrival in enumerate(arrivals):
    pick = arrival.pick
    time = pick.time
    station = pick.waveform_id.station_code
    phase = arrival.phase
    distance = arrival.distance

    times.append(time)
    distances.append(distance)

    if phase == 'P':
        color = 'r'
    else:
        color = 'b'

    # plt.vlines(time, distance - scale, distance + scale, color=color)
    plt.vlines(time - arrival.time_residual, distance - scale, distance +
               scale, color=color, linestyle = 'dotted')

    if station in stations:
        continue
    stations.append(station)
    tr = st_comp.select(station=station)
    if not tr:
        continue

    tr = tr[0].detrend('demean')
    rts = np.arange(0, len(tr)) / tr.stats.sampling_rate
    tr_starttime = tr.stats.starttime
    t = [tr_starttime + timedelta(seconds=rt) for rt in rts]
    data = tr.data / np.max(tr.data) * 20 + distance
    plt.plot(t, data, 'k')

#plt.tight_layout()
ax.format_xdata = mdates.DateFormatter('%H:%M:%s.%f')
plt.ylabel('distance (m)')
plt.xlabel('time')
plt.show()

