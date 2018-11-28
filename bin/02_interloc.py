
from importlib import reload
import numpy as np
import os
# import glob
# from xseis2 import xutil
# from xseis2 import xflow
from xseis2 import xspy
# from datetime import datetime, timedelta

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

logger = app.logger(params.log_topic, params.log_file_name)

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
    tr = st[0]
    print(tr.stats.starttime, tr.stats.endtime, len(tr))

    st.zpad_names()
    data, sr, t0 = st.as_array(wlen_sec)
    data = tools.decimate(data, sr, int(sr / dsr))
    chanmap = st.chanmap().astype(np.uint16)
    ikeep = htt.index_sta(st.unique_stations())

    npz_file = os.path.join(logdir, "iloc_" + str(t0) + ".npz")
    out = xspy.pySearchOnePhase(data, dsr, chanmap, stalocs[ikeep], tt_ptrs[ikeep],
                                 ngrid, nthreads, debug, npz_file)
    vmax, imax, iot = out
    lmax = htt.icol_to_xyz(imax)

    ot_epoch = tools.datetime_to_epoch_sec((t0 + iot / dsr).datetime)

    logger.info("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
    logger.info("utm_loc: ", lmax.astype(int))

    if vmax > params.threshold:
        logger.info("=======================================\n")
        logger.info("VMAX over threshold (%.3f > %.3f)" %
					(vmax, params.threshold))
        logger.info("====== sending message ================\n")

        key_out = ("iloc_%.4f" % (ot_epoch)).encode('utf-8')
        msg_out = msgpack.pack([ot_epoch, lmax[0], lmax[1], lmax[2], vmax, st])
        kafka_handler_obj = KafkaHandler(brokers)
        kafka_handler_obj.send_to_kafka(topic_out, key_out, msg_out)
        kafka_handler_obj.producer.flush()
    logger.info("\n")
