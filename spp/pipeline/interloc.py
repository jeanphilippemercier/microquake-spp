import os
from datetime import datetime
from time import time

import numpy as np

from microquake.core import UTCDateTime, AttribDict
from microquake.core.event import Origin
from microquake.core.util import tools
from xseis2 import xspy


def process(
        cat=None,
        stream=None,
        logger=None,
        app=None,
        module_settings=None,
        prepared_objects=None,
):
    detection_threshold = module_settings.detection_threshold
    nthreads = module_settings.nthreads
    fixed_wlen_sec = module_settings.fixed_wlen_sec
    samplerate_decimated = module_settings.samplerate_decimated
    pair_dist_min = module_settings.pair_dist_min
    pair_dist_max = module_settings.pair_dist_max
    cc_smooth_length_sec = module_settings.cc_smooth_length_sec
    debug_level = module_settings.debug_level
    debug_file_dir = module_settings.debug_file_dir

    whiten_corner_freqs = np.array(module_settings.whiten_corner_freqs,
                                   dtype=np.float32)

    htt = app.get_ttable_h5()
    stalocs = htt.locations
    ttable = (htt.hf["ttp"][:] * samplerate_decimated).astype(np.uint16)
    ngrid = ttable.shape[1]
    ttable_row_ptrs = np.array(
        [row.__array_interface__["data"][0] for row in ttable])

    logger.info("preparing data for Interloc")
    t4 = time()
    st_out = stream.copy()

    # remove channels which do not have matching ttable entries
    # This should be handled upstream
    for trace in stream:
        if trace.stats.station not in htt.stations:
            logger.info('removing trace for station %s' % trace.stats.station)
            stream.remove(trace)
        elif np.max(trace.data) == 0:
            logger.info('removing trace for station %s' % trace.stats.station)
            stream.remove(trace)
        elif trace.stats.station in app.settings.sensors.black_list:
            logger.info('removing trace for station %s' % trace.stats.station)
            stream.remove(trace)

    data, samplerate, t0 = stream.as_array(fixed_wlen_sec)
    logger.info("data: %s", data)
    data = np.nan_to_num(data)
    decimate_factor = int(samplerate / samplerate_decimated)
    data = tools.decimate(data, samplerate, decimate_factor)
    channel_map = stream.channel_map().astype(np.uint16)

    ikeep = htt.index_sta(stream.unique_stations())
    debug_file = os.path.join(debug_file_dir, "iloc_" + str(t0) + ".npz")
    t5 = time()
    logger.info(
        "done preparing data for Interloc in %0.3f seconds" % (t5 - t4))

    logger.info("Locating event with Interloc")
    t6 = time()
    logger.info(
        "data %s,samplerate_decimated %s,channel_map %s,stalocs[ikeep] %s,"
        " ttable_row_ptrs[ikeep] %s,ngrid %s,nthreads %s,debug %s,debug_"
        "file %s", data, samplerate_decimated, channel_map, stalocs[ikeep],
        ttable_row_ptrs[ikeep], ngrid, nthreads, debug_level, debug_file)

    out = xspy.pySearchOnePhase(
        data,
        samplerate_decimated,
        channel_map,
        stalocs[ikeep],
        ttable_row_ptrs[ikeep],
        ngrid,
        whiten_corner_freqs,
        pair_dist_min,
        pair_dist_max,
        cc_smooth_length_sec,
        nthreads,
        debug_level,
        debug_file
    )

    vmax, imax, iot = out
    lmax = htt.icol_to_xyz(imax)
    t7 = time()
    logger.info(
        "Done locating event with Interloc in %0.3f seconds" % (t7 - t6))

    ot_epoch = tools.datetime_to_epoch_sec(
        (t0 + iot / samplerate_decimated).datetime)
    utcdatetime = UTCDateTime(datetime.fromtimestamp((ot_epoch)))
    method = "%s" % ("INTERLOC",)
    cat[0].origins.append(
        Origin(x=lmax[0], y=lmax[1], z=lmax[2], time=utcdatetime,
               method_id=method)
    )
    cat[0].preferred_origin_id = cat[0].origins[-1].resource_id.id

    logger.info("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
    logger.info("utm_loc: %r", lmax.astype(int))

    logger.info("=======================================\n")
    logger.info(
        "VMAX over threshold (%.3f > %.3f)" % (vmax, detection_threshold))

    logger.info("IMS location %s" % cat[0].origins[0].loc)
    logger.info("Interloc location %s" % cat[0].origins[1].loc)
    dist = np.linalg.norm(cat[0].origins[0].loc - cat[0].origins[1].loc)
    logger.info("distance between two location %0.2f m" % dist)

    cat[0].preferred_origin().extra.interloc_vmax \
        = AttribDict({'value': vmax, 'namespace': 'MICROQUAKE'})

    normed_vmax = vmax * fixed_wlen_sec

    cat[0].preferred_origin().extra.interloc_normed_vmax \
        = AttribDict({'value': normed_vmax, 'namespace': 'MICROQUAKE'})

    return cat, st_out
