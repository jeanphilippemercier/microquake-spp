import os
from datetime import datetime
from time import time

import numpy as np

from microquake.core import UTCDateTime
from microquake.core.event import Origin
from microquake.core.util import tools
from spp.utils.cli import CLI
from xseis2 import xspy


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):
    threshold = module_settings.threshold
    npz_file_dir = module_settings.npz_file_dir
    nthreads = module_settings.nthreads
    wlen_sec = module_settings.wlen_sec
    debug = module_settings.debug
    dsr = module_settings.dsr

    htt = app.get_ttable_h5()
    stalocs = htt.locations
    ttable = (htt.hf["ttp"][:] * dsr).astype(np.uint16)
    ngrid = ttable.shape[1]
    tt_ptrs = np.array([row.__array_interface__["data"][0] for row in ttable])

    logger.info("preparing data for Interloc")
    t4 = time()
    st_out = stream.copy()

    # remove channels which do not have matching ttable entries
    for trace in stream:
        if trace.stats.station not in htt.stations:
            stream.remove(trace)

    data, sr, t0 = stream.as_array(wlen_sec)
    logger.info("data: %s", data)
    data = np.nan_to_num(data)
    data = tools.decimate(data, sr, int(sr / dsr))
    chanmap = stream.chanmap().astype(np.uint16)

    ikeep = htt.index_sta(stream.unique_stations())
    npz_file = os.path.join(npz_file_dir, "iloc_" + str(t0) + ".npz")
    t5 = time()
    logger.info("done preparing data for Interloc in %0.3f seconds" % (t5 - t4))

    logger.info("Locating event with Interloc")
    t6 = time()
    logger.info("data %s,dsr %s,chanmap %s,stalocs[ikeep] %s,tt_ptrs[ikeep] %s,ngrid %s,nthreads %s,debug %s,npz_file %s", data,dsr,chanmap,stalocs[ikeep],tt_ptrs[ikeep],ngrid,nthreads,debug,npz_file)

    out = xspy.pySearchOnePhase(
        data,
        dsr,
        chanmap,
        stalocs[ikeep],
        tt_ptrs[ikeep],
        ngrid,
        nthreads,
        debug,
        npz_file,
    )
    vmax, imax, iot = out
    lmax = htt.icol_to_xyz(imax)
    t7 = time()
    logger.info("Done locating event with Interloc in %0.3f seconds" % (t7 - t6))

    ot_epoch = tools.datetime_to_epoch_sec((t0 + iot / dsr).datetime)
    utcdatetime = UTCDateTime(datetime.fromtimestamp((ot_epoch)))
    method = "%s" % ("INTERLOC",)
    cat[0].origins.append(
        Origin(x=lmax[0], y=lmax[1], z=lmax[2], time=utcdatetime, method_id=method)
    )
    cat[0].preferred_origin_id = cat[0].origins[-1].resource_id.id

    logger.info("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
    logger.info("utm_loc: %r", lmax.astype(int))

    logger.info("=======================================\n")
    logger.info("VMAX over threshold (%.3f > %.3f)" % (vmax, threshold))

    logger.info("IMS location %s" % cat[0].origins[0].loc)
    logger.info("Interloc location %s" % cat[0].origins[1].loc)
    dist = np.linalg.norm(cat[0].origins[0].loc - cat[0].origins[1].loc)
    logger.info("distance between two location %0.2f m" % dist)

    return cat, st_out


__module_name__ = "interloc"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
