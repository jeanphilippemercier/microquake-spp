from spp.travel_time import core
from spp.utils import get_stations
from datetime import datetime
from microquake.core import ctl
from microquake.core import read, UTCDateTime
from microquake.core.stream import Stream
from microquake.core.event import Arrival, Event, Origin
from microquake.core.event import ResourceIdentifier
from obspy.core.event import Event as obsEvent
from microquake.waveform.pick import SNR_picker, calculate_snr, kurtosis_picker
import numpy as np
import os

from importlib import reload
reload(core)

file = '20180707153222.mseed'
file = '20180707153222_trim.mseed'
#file = '/mnt/seismic_shared_storage/continuous_data/20180523_185101_900000_20180523_185103_100000.mseed'
st = read(file, format='MSEED')
for tr in st:
	print(tr.get_id(), tr.stats.starttime, tr.stats.endtime, tr.stats.npts, tr.data.size)
exit()

starttime = UTCDateTime('2018-07-07T15:32:22.3Z')
endtime   = starttime + 1.2
#endtime   = starttime + 1.0000001
st.trim(starttime, endtime, pad=True, fill_value=0.0)
for tr in st:
	#data = tr.data[:6000]
	#tr.data = data
	print(tr.get_id(), tr.stats.starttime, tr.stats.endtime, tr.stats.npts, tr.data.size)
st.write('20180707153222_trim.mseed')

