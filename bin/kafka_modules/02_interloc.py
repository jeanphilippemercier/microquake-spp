
from importlib import reload
import numpy as np
import os
import glob

from xseis2 import xutil
from xseis2 import xflow
from xseis2 import xspy
from datetime import datetime, timedelta

from spp.utils.kafka import KafkaHandler

from microquake.io import msgpack
from microquake.core.util import tools
from microquake.core import read
from io import BytesIO

from spp.utils.application import Application


app = Application()
params = app.settings.interloc
nthreads = params.threads
wlen_sec = params.wlen_seconds
debug = params.debug
dsr = params.dsr

logdir = os.path.join(app.common_dir, "dump")

brokers = app.settings.kafka.brokers
topic_in = params.kafka_consumer_topic
topic_out = params.kafka_producer_topic
kaf_handle = KafkaHandler(brokers)

htt = app.get_ttable_h5()
stalocs = htt.locations
ttable = (htt.hf['ttp'][:] * dsr).astype(np.uint16)
ngrid = ttable.shape[1]
tt_ptrs = np.array([row.__array_interface__['data'][0] for row in ttable])


consumer = KafkaHandler.consume_from_topic(topic_in, brokers, group_id=None)

print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
	print("Received Key:", msg_in.key)

	# st = msgpack.unpack(msg_in.value)
	st = read(BytesIO(msg_in.value))

	# print(st)
	xflow.prep_stream(st)
	chanmap = st.chanmap().astype(np.uint16)

	ikeep = htt.index_sta(st.unique_stations())
	data, t0, stations, chanmap = xflow.build_input_data(st, wlen_sec, dsr)

	npz_file = os.path.join(logdir, "iloc_" + str(t0) + ".npz")
	out = xspy.pySearchOnePhase(data, dsr, chanmap, stalocs[ikeep], tt_ptrs[ikeep],
								 ngrid, nthreads, debug, npz_file)
	vmax, imax, iot = out
	lmax = htt.icol_to_xyz(imax)

	ot_epoch = tools.datetime_to_epoch_sec((t0 + iot / dsr).datetime)

	print("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
	print("utm_loc: ", lmax.astype(int))

	if vmax > params.threshold:
		print("VMAX over threshold, sending message")

		key_out = ("iloc_%.4f" % (ot_epoch)).encode('utf-8')
		msg_out = msgpack.pack([ot_epoch, lmax[0], lmax[1], lmax[2], vmax, st])
		# msg_out, key_out = xflow.encode_for_kafka(ot_epoch, lmax, vmax)
		kafka_handler_obj = KafkaHandler(brokers)
		kafka_handler_obj.send_to_kafka(topic_out, key_out, msg_out)
		kafka_handler_obj.producer.flush()
	print("=============================================\n")

