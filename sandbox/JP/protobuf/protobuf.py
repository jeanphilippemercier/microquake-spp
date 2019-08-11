import waveform_pb2
from microquake.core import read, read_events
from base64 import b64encode, b64decode
from time import time
from importlib import reload
import numpy as np
# import colorama
from glob import glob
# colorama.init()

# protoc -I=. --python_out=. --java_out=. waveform.proto
# reload(waveform_pb2)

t0 = time()
st = read('2019-01-07T20-39-25.549443Z.mseed')
t1 = time()
cat = read_events('2019-01-07T20-39-25.549443Z.xml', format='QUAKEML')
print('Reading mseed file in %0.3f' % (t1 - t0))

if cat[0].preferred_origin_id:
    origin = cat[0].preferred_origin()
else:
    origin = cat[0].origin[0]


stream = waveform_pb2.Stream()
stream.time = str(origin.time)

stations = st.unique_stations()
picks = {}
for station in stations:
    picks[station] = {'p': {}, 's': {}, 'distance': 0}
    picks[station]['p'] = {'pick_time': '', 'residual': 0, 'e_s_distance': 0}
    picks[station]['s'] = {'pick_time': '', 'residual': 0, 'e_s_distance': 0}

for arrival in origin.arrivals:
    station = arrival.get_pick().waveform_id.station_code
    if station not in picks.keys():
        continue
    picks[station][arrival.phase.lower()]['pick_time'] = \
        str(arrival.get_pick().time)
    picks[station][arrival.phase.lower()]['residual'] = arrival.time_residual
    picks[station]['distance'] = arrival.distance

t0 = time()
with open('waveform.pb', mode='wb') as wf_file:
    for station in stations:
        waveform = stream.traces.add()
        st_sta = st.select(station=station).composite()
        # check for NaN
        # if any(np.isnan(st_sta[0].data)):
        #     continue

        npts = st[0].stats.npts
        waveform.npts = st[0].stats.npts
        waveform.starttime = str(st[0].stats.starttime)
        waveform.sampling_rate = int(st[0].stats.sampling_rate)
        data = b64encode(st[0].data.tobytes())
        waveform.data = data
        waveform.station_code = station
        waveform.p_pick = picks[station]['p']['pick_time']
        waveform.s_pick = picks[station]['s']['pick_time']
        waveform.p_residual = picks[station]['p']['residual']
        waveform.e_s_distance = picks[station]['distance']
        waveform.channel = 'C'

    wf_file.write(stream.SerializeToString())

t1 = time()

print('Writing all the protobuf file in %0.3f' % (t1 - t0))

t0 = time()
st_in = waveform_pb2.Stream()
st_in.ParseFromString(open('waveform.pb', 'rb').read())

for trace in st_in.traces:
    data = np.frombuffer(b64decode(trace.data), dtype=np.float32)
# for fle in glob('*.pb'):
#     wf = waveform_pb2.Waveform()
#     wf.ParseFromString(open(fle, 'rb').read())
#     data = np.frombuffer(b64decode(wf.data), dtype=np.float32)
t1 = time()

print('Reading all the protobuf files in %0.3f' % (t1 - t0))


