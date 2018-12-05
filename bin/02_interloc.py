
from importlib import reload
import numpy as np
import os
# import glob
# from xseis2 import xutil
# from xseis2 import xflow
from xseis2 import xspy
from microquake.core import Stream, UTCDateTime
from microquake.core.event import Origin
# from datetime import datetime, timedelta
from datetime import datetime
import uuid

from microquake.io import msgpack
from microquake.core.util import tools
from microquake.core import read, read_events
from io import BytesIO
from time import time

from spp.utils.application import Application


app = Application()
params = app.settings.interloc
nthreads = params.threads
wlen_sec = params.wlen_seconds
debug = params.debug
dsr = params.dsr

logger = app.get_logger(params.log_topic, params.log_file_name)

logger.info('setting up Kafka')
k_producer = app.get_kafka_producer(logger=logger)
k_consumer = app.get_kafka_consumer(logger=logger)
k_consumer.subscribe([app.settings.interloc.kafka_consumer_topic])
logger.info('done setting up Kafka')

logger.info('init connection to redis')
redis_conn = app.init_redis()
logger.info('connectiong to redis database successfully initated')

htt = app.get_ttable_h5()
stalocs = htt.locations
ttable = (htt.hf['ttp'][:] * dsr).astype(np.uint16)
ngrid = ttable.shape[1]
tt_ptrs = np.array([row.__array_interface__['data'][0] for row in ttable])

log_dir = '/app/common'

print("Awaiting Kafka messsages")
while True:
    msg_in = k_consumer.poll(timeout=1)
    if msg_in is None:
        continue
    if msg_in.value() == b'Broker: No more messages':
        continue

    redis_key = msg_in.value()
    logger.info('getting data from Redis (key:%s)' % redis_key)
    t0 = time()
    data = msgpack.unpack(redis_conn.get(redis_key))
    t1 = time()
    logger.info('done getting data from Redis in %0.3f seconds' % (t1 - t0))

    logger.info('unpacking data')
    t2 = time()
    st = data[1]
    cat = read_events(BytesIO(data[0]), format='QUAKEML')
    tr = st[0]
    t3 = time()
    logger.info('done unpacking data in %0.3f seconds' % (t3 - t2))
    logger.info('%s, %s, %s' % (tr.stats.starttime, tr.stats.endtime, len(tr)))

    logger.info('preparing data for Interloc')
    t4 = time()
    st_out = st.copy()
    data, sr, t0 = st.as_array(wlen_sec)
    data = np.nan_to_num(data)
    data = tools.decimate(data, sr, int(sr / dsr))
    chanmap = st.chanmap().astype(np.uint16)
    ikeep = htt.index_sta(st.unique_stations())
    npz_file = os.path.join(log_dir, "iloc_" + str(t0) + ".npz")
    t5 = time()
    logger.info('done preparing data for Interloc in %0.3f seconds' % (t5 -
                                                                        t4))

    logger.info('Locating event with Interloc')
    t6 = time()
    out = xspy.pySearchOnePhase(data, dsr, chanmap, stalocs[ikeep], tt_ptrs[ikeep],
                                 ngrid, nthreads, debug, npz_file)
    vmax, imax, iot = out
    lmax = htt.icol_to_xyz(imax)
    t7 = time()
    logger.info('Done locating event with Interloc in %0.3f seconds' % (t7 -
                                                                        t6))

    ot_epoch = tools.datetime_to_epoch_sec((t0 + iot / dsr).datetime)
    time = UTCDateTime(datetime.fromtimestamp((ot_epoch)))
    cat[0].origins.append(Origin(x=lmax[0], y=lmax[1], z=lmax[2], time=time))
    cat[0].preferred_origin_id = cat[0].origins[-1].resource_id.id

    logger.info("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
    logger.info("utm_loc: ", lmax.astype(int))

    # if vmax > params.threshold:
    logger.info("=======================================\n")
    logger.info("VMAX over threshold (%.3f > %.3f)" %
                (vmax, params.threshold))
    logger.info("====== sending message ================\n")

    key_out = ("iloc_%.4f" % (ot_epoch)).encode('utf-8')
    # msg_out = msgpack.pack([ot_epoch, lmax[0], lmax[1], lmax[2], vmax, st])
    ev_io = BytesIO()
    cat.write(ev_io, format='QUAKEML')
    msg_out = msgpack.pack([ev_io.getvalue(), st_out])
    redis_key = str(uuid.uuid4())
    redis_conn.set(redis_key, msg_out)
    k_producer.produce(app.settings.interloc.kafka_producer_topic, redis_key)
    logger.info("\n")
