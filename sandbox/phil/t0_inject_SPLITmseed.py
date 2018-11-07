"""
Injects local mseed files into kafka queue
"""
import numpy as np
import os
# import struct
import glob
from importlib import reload
# import datetime
from obspy import read
# from xseis2 import xutil
from io import BytesIO
from spp.utils.kafka import KafkaHandler
import yaml
from datetime import datetime
from struct import unpack
# import time


def chunk_mseed(mseed_bytes, mseed_reclen=4096):

	starts = np.arange(0, len(mseed_bytes), mseed_reclen)
	keys = []
	blobs = []

	for start in starts:
	    end = start + mseed_reclen
	    chunk = mseed_bytes[start:end]

	    y = unpack('>H', chunk[20:22])[0]
	    DoY = unpack('>H', chunk[22:24])[0]
	    H = unpack('>B', chunk[24:25])[0]
	    M = unpack('>B', chunk[25:26])[0]
	    S = unpack('>B', chunk[26:27])[0]
	    # r = unpack('>B', chunk[27:28])[0]
	    ToMS = unpack('>H', chunk[28:30])[0]

	    dt = datetime.strptime('%s/%0.3d %0.2d:%0.2d:%0.2d.%0.3d'
	                           % (y, DoY, H, M, S, ToMS),
	                           '%Y/%j %H:%M:%S.%f')
	    key = dt.strftime('%Y-%d-%m %H:%M:%S.%f').encode('utf-8')
	    # key = dt.strftime('%Y-%d-%m %H:%M:%S.%f')
	    keys.append(key)
	    blobs.append(chunk)

	# blobs = np.array(blobs)
	keys = np.array(keys)

	utimes = np.unique(keys)
	groups = []
	for key in utimes:
		groups.append(np.where(keys == key)[0])

	newdat = []
	for ig, group in enumerate(groups):
	    data = b''
	    for ix in group:
	        data += blobs[ix]
	    newdat.append(data)

	return utimes, newdat


# data_src = "/mnt/seismic_shared_storage/OT_seismic_data/"
data_src = "/home/phil/data/oyu/synthetic/chunks/"
# data_src = params['data_connector']['data_source']['location']
MSEEDS = np.sort(glob.glob(data_src + '*.mseed'))


config_dir = os.environ['SPP_CONFIG']
fname = os.path.join(config_dir, 'data_connector_config.yaml')

with open(fname, 'r') as cfg_file:
	params = yaml.load(cfg_file)
# Create Kafka Object
brokers = params['data_connector']['kafka']['brokers']
# topic = params['data_connector']['kafka']['topic']
kaf_handle = KafkaHandler(brokers)

topic = "mseed-blocks"

print("Sending kafka mseed messsages")

for fle in MSEEDS[:3]:
	fsizemb = os.path.getsize(fle) / 1024.**2
	print("%s | %.2f mb" % (os.path.basename(fle), fsizemb))
	if (fsizemb < 3 or fsizemb > 15):
		print("skipping file")
		continue

	st = read(fle)  # could read fle directly into bytes io
	buf = BytesIO()
	st.write(buf, format='MSEED')
	# mseed_bytes = output.getvalue()
	mseed_reclen = st[0].stats.mseed.record_length
	tkeys, bchunks = chunk_mseed(buf.getvalue(), mseed_reclen)
	# np.array(rgb).reshape(640, 360, 3)
	# chunk = mseed_bytes[0:mseed_reclen]
	# st2 = read(BytesIO(chunk))

	for i, chunk in enumerate(bchunks):
		msg_out = chunk
		key_out = bytes(tkeys[i])
		kaf_handle.send_to_kafka(topic, msg_out, key_out)
		kaf_handle.producer.flush()
	print("==================================================================")
