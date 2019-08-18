from importlib import reload
import numpy as np
import matplotlib.pyplot as plt

# from xseis2 import xutil
from microquake.core.event import Arrival, Event, Origin, Pick
from microquake.core.event import ResourceIdentifier
# from microquake.core import Stream
# from microquake.core import Trace
from microquake.core import read
from obspy import UTCDateTime
from spp.utils.application import Application

from microquake.core.util import tools
from microquake.core.util import plotting as qplot
from microquake.io import msgpack
from spp.utils.kafka import KafkaHandler
import os
plt.ion()


app = Application()
params = app.settings.picker

ix_grid = 388036
ix_ot = 1062
ot_epoch = 0.177
sloc = np.array([651600, 4767420, 200])
ot_dtime = UTCDateTime(ot_epoch)


mseed_file = os.path.join(app.common_dir, 'synthetic', 'sim_dat_noise.mseed')
st = read(mseed_file)

# st.filter('bandpass', freqmin=50, freqmax=300)
stcomp = st.composite()
keys = stcomp.unique_stations()

htt = app.get_ttable_h5()

ix_grid2 = htt.xyz_to_icol(sloc)

ista = htt.index_sta(stcomp.unique_stations())
ttP = htt.hf['ttp'][ista, ix_grid]
ttS = htt.hf['tts'][ista, ix_grid]

ptimes_p = np.array([ot_dtime + tt for tt in ttP])
ptimes_s = np.array([ot_dtime + tt for tt in ttS])

picks = tools.make_picks(stcomp, ptimes_p, 'P', params)
picks += tools.make_picks(stcomp, ptimes_s, 'S', params)

# snrs = np.array([pick.snr for pick in picks])
# plt.plot(snrs)

qplot.stream(stcomp, picks=picks, color='black', alpha=0.6)

arrivals = [Arrival(phase=p.phase_hint, pick_id=p.resource_id) for p in picks]

og = Origin(time=ot_dtime, x=sloc[0], y=sloc[1], z=sloc[2], arrivals=arrivals)
event = Event(origins=[og], picks=picks)
event.preferred_origin_id = og.resource_id


pack = msgpack.pack([event, st])





t0 = st[0].stats.starttime
ot_epoch = tools.datetime_to_epoch_sec((t0 + iot / dsr).datetime)



pfle = 'pack.dat'
f = open(pfle, 'wb')
f.write(d1)
f.close()


f = open(pfle, 'rb')
# print(f.read())
d3 = f.read()

# d2 = msgpack.unpack(d1)
d2 = msgpack.unpack(d3)


p = picks1[ix]
ix = 80
tr = stcomp[ix]
p1 = picks1[ix].time - tr.stats.starttime
p2 = picks2[ix].time - tr.stats.starttime

times = tr.times()

plt.plot(times, tr.data)
plt.axvline(p1, color='red')
plt.axvline(p2, color='red')



# snr_wlens = np.array([15e-3, 10e-3])
# wlen_search = 50e-3
# stepsize = 1e-3


def make_picks(stcomp, pick_times_utc, phase, pick_params):
	snr_wlens = np.array(pick_params.snr_wlens)
	wlen_search = pick_params.wlen_search
	stepsize = pick_params.stepsize

	picks = []
	for tr, ptime in zip(stcomp, pick_times_utc):
		picks.append(tr.make_pick(ptime, wlen_search,
					stepsize, snr_wlens, phase_hint=phase))

	return picks


def picks_to_dict(picks):
	pd = {}
	for p in picks:
		key = p.waveform_id.get_seed_string()
		if key not in pd:
			pd[key] = []
		pd[key].append(p.time)
	return pd



# tts_dir = os.path.join(app.common_dir, "NLL/time")
# ttP1, locs, ndict, gdef = xutil.ttable_from_nll_grids(tts_dir, key="OT.P")
# ttS1, locs, ndict, gdef = xutil.ttable_from_nll_grids(tts_dir, key="OT.S")
# ikeep = np.array([ndict[k] for k in st.unique_stations()])
# ttP1 = ttP1[ikeep, ix_grid]

# ttS = ttS[ikeep, ix_grid]

# pd = picks_to_dict(picks)


# def plot_picks(stcomp, picks):

def plot_stream(st, picks=None, spacing=1.2, **kwargs):

	# import matplotlib.lines as mlines
	pd = picks_to_dict(picks)

	shifts = np.arange(0, len(st), 1) * spacing
	times = st[0].times()
	sr = st[0].stats.sampling_rate
	vsize = (shifts[1] - shifts[0]) / 4

	for i, tr in enumerate(st):
		shift = shifts[i]
		sig = tr.data
		tmp = sig / np.max(np.abs(sig)) + shift
		plt.plot(times, tmp, **kwargs)
		plt.text(0, shift + 0.1, tr.id, fontsize=10)

		if picks is not None:
			trp = pd[tr.id]
			pixs = [tr.time_to_index(pt) / sr for pt in trp]
			for pick in pixs:
				xv = [pick, pick]
				yv = [shift - vsize, shift + vsize]
				plt.plot(xv, yv, color='red')

