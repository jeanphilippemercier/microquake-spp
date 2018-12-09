import numpy as np
import os
from microquake.core import Stream, UTCDateTime
from microquake.core.event import Origin
from datetime import datetime
from microquake.core.util import tools
from spp.utils.application import Application


def callback(cat=None, stream=None, extra_msgs=None, logger=None,
             wlen_sec=None, log_dir=None, dsr=None, threshold=None):

    from time import time
    from xseis2 import xspy

    logger.info('preparing data for Interloc')
    t4 = time()
    st_out = stream.copy()
    data, sr, t0 = stream.as_array(wlen_sec)
    data = np.nan_to_num(data)
    data = tools.decimate(data, sr, int(sr / dsr))
    chanmap = stream.chanmap().astype(np.uint16)
    ikeep = htt.index_sta(stream.unique_stations())
    npz_file = os.path.join(log_dir, "iloc_" + str(t0) + ".npz")
    t5 = time()
    logger.info('done preparing data for Interloc in %0.3f seconds' % (t5 -
                                                                        t4))

    logger.info('Locating event with Interloc')
    t6 = time()
    out = xspy.pySearchOnePhase(data, dsr, chanmap, stalocs[ikeep],
                                tt_ptrs[ikeep], ngrid, nthreads, debug,
                                npz_file)
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

    logger.info("=======================================\n")
    logger.info("VMAX over threshold (%.3f > %.3f)" %
                (vmax, threshold))

    return cat, st_out


__module_name__ = 'interloc'

app = Application(module_name=__module_name__)
app.init_module()

params = app.settings.interloc
nthreads = params.threads
wlen_sec = params.wlen_seconds
debug = params.debug
dsr = params.dsr

conf = {'wlen_sec': wlen_sec, 'log_dir': '/app/common', 'dsr': dsr,
        'threshold': params.threshold}

htt = app.get_ttable_h5()
stalocs = htt.locations
ttable = (htt.hf['ttp'][:] * dsr).astype(np.uint16)
ngrid = ttable.shape[1]
tt_ptrs = np.array([row.__array_interface__['data'][0] for row in ttable])

app.logger.info('awaiting message from Kafka')
while True:
    msg_in = app.consumer.poll(timeout=1)
    if msg_in is None:
        continue
    if msg_in.value() == b'Broker: No more messages':
        continue
    try:
        cat, st = app.receive_message(msg_in, callback, **conf)
    except Exception as e:
        app.logger.error(e)


    app.send_message(cat, st)
    app.logger.info('awaiting message from Kafka')
