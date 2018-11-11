
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
from spp.utils.application import Application


app = Application()
nthreads = app.settings.interloc.threads
wlen_sec = app.settings.interloc.wlen_seconds
dsr = app.settings.interloc.dsr
debug = app.settings.interloc.debug

brokers = app.settings.kafka.brokers
topic_in = app.settings.interloc.kafka_consumer_topic
topic_out = app.settings.interloc.kafka_producer_topic

kaf_handle = KafkaHandler(brokers)

tts_dir = os.path.join(app.common_dir, "NLL/time")

ttable, stalocs, namedict, gdef = xutil.ttable_from_nll_grids(tts_dir, key="OT.P")
# ttable, stalocs, namedict, gdef = xutil.ttable_from_nll_grids(tts_path, key="OT.S")
ttable = (ttable * dsr).astype(np.uint16)
ngrid = ttable.shape[1]
tt_ptrs = np.array([row.__array_interface__['data'][0] for row in ttable])

consumer = KafkaHandler.consume_from_topic(topic_in, brokers, group_id='group')

print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
	print("Received Key:", msg_in.key)

	st = msgpack.unpack(msg_in.value)

	print(st)
	xflow.prep_stream(st)
	data, t0, stations, chanmap = xflow.build_input_data(st, wlen_sec, dsr)
	ikeep = np.array([namedict[k] for k in stations])
	logdir = ''
	npz_file = os.path.join(logdir, "iloc_" + str(t0) + ".npz")
	out = xspy.pySearchOnePhase(data, dsr, chanmap, stalocs[ikeep], tt_ptrs[ikeep],
								 ngrid, nthreads, debug, npz_file)
	vmax, imax, iot = out
	lmax = xutil.imax_to_xyz_gdef(imax, gdef)
	# ot_epoch = (t0 + iot / dsr).datetime.timestamp()

	ot_epoch = ((t0 + iot / dsr).datetime - datetime(1970, 1, 1)) / timedelta(seconds=1)

	print("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
	print("utm_loc: ", lmax.astype(int))

	# print("Sending Kafka interloc messsage")
	# msg_out, key_out = xflow.encode_for_kafka(ot_epoch, lmax, vmax)
	# kafka_handler_obj = KafkaHandler(brokers)
	# kafka_handler_obj.send_to_kafka(topic_out, msg_out, key_out)
	# kafka_handler_obj.producer.flush()
	# print("==================================================================")

