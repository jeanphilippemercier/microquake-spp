import waveform_pb2
from microquake.core import read, read_events
from base64 import b64encode, b64decode
from time import time
from importlib import reload
import numpy as np
import colorama
from glob import glob
colorama.init()
# reload(waveform_pb2)

t0 = time()
st = read('2019-01-08T04_50_06.392876Z.mseed')
t1 = time()
cat = read_events('2019-01-08T04_50_06.392876Z.xml', format='QUAKEML')
print('Reading mseed file in %0.3f' % (t1 - t0))

if cat[0].preferred_origin_id:
    origin = cat[0].preferred_origin()
else:
    origin = cat[0].origin[0]

stations = st.unique_stations()
picks = {}
for station in stations:
    picks[station] = {'p': '', 's': ''}

for arrival in origin.arrivals:
    station = arrival.get_pick().waveform_id.station_code
    if station not in picks.keys():
        continue
    picks[station][arrival.phase.lower()] = str(arrival.get_pick().time)

t0 = time()
for station in stations:
    with open('test_pb_%s.waveform' % station, mode='wb') as wf_file:
        st_sta = st.select(station=station).composite()

        # check for NaN
        if any(np.isnan(st_sta[0].data)):
            continue

        npts = st[0].stats.npts
        waveform = waveform_pb2.Waveform()
        waveform.npts = st[0].stats.npts
        waveform.starttime = str(st[0].stats.starttime)
        waveform.sampling_rate = int(st[0].stats.sampling_rate)
        data = b64encode(st[0].data.tobytes())
        waveform.data = data
        waveform.station_code = station
        waveform.p_pick = picks[station]['p']
        waveform.s_pick = picks[station]['s']

        wf_file.write(waveform.SerializeToString())

t1 = time()

print('Writing all the protobuf file in %0.3f' % (t1 - t0))

t0 = time()
for fle in glob('*.waveform'):
    wf = waveform_pb2.Waveform()
    wf.ParseFromString(open(fle, 'rb').read())
    data = np.frombuffer(b64decode(wf.data), dtype=np.float32)
t1 = time()

print('Reading all the protobuf files in %0.3f' % (t1 - t0))


