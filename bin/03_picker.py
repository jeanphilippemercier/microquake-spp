#!/usr/bin/env python3
"""
Predict Picks
"""

from time import time

import numpy as np

from microquake.core import UTCDateTime
from microquake.core.event import CreationInfo, Origin
from microquake.waveform.pick import snr_picker
from microquake.core.stream import is_valid
from spp.utils.cli import CLI


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):
    """
    Predict picks for event
    """

    freq_min = module_settings.waveform_filter.frequency_min
    freq_max = module_settings.waveform_filter.frequency_max
    residual_tolerance = module_settings.residual_tolerance

    st_in = stream.copy().detrend("demean")

    logger.info('cleaning the input stream')
    st = is_valid(st_in, return_stream=True, freqmin=freq_min,
                  freqmax=freq_max)
    logger.info('done cleaning the input stream. %d of %d stations kept.' %
                (len(st.unique_stations()), len(stream.unique_stations())))

    st = st.taper(max_percentage=0.1, max_length=0.01)
    st = st.filter("bandpass", freqmin=freq_min, freqmax=freq_max)

    ot_utcs = []
    logger.info("calculating origin time")
    t0 = time()
    loc = cat[0].preferred_origin().loc
    ot_utcs.append(app.estimate_origin_time(stream, loc))
    t1 = time()
    logger.info("done calculating origin time in %0.3f seconds" % (t1 - t0))

    ot_utc_interloc = None
    for origin in cat[0].origins:
        if origin.method_id == 'smi:local/INTERLOC':
            ot_utc_interloc = origin.time

    # only appending the last one
    if ot_utc_interloc is not None:
        ot_utcs.append(ot_utc_interloc)

    snr_picks_filtered_list = []
    snr_picks_len = []

    for ot_utc in ot_utcs:
        logger.info("predicting picks for origin time %s" % ot_utc)
        t2 = time()
        o_loc = cat[0].preferred_origin().loc
        picks = app.synthetic_arrival_times(o_loc, ot_utc)
        t3 = time()
        logger.info("done predicting picks in %0.3f seconds" % (t3 - t2))

        logger.info("picking P-waves")
        t4 = time()
        search_window = np.arange(
            module_settings.p_wave.search_window.start,
            module_settings.p_wave.search_window.end,
            module_settings.p_wave.search_window.resolution,
        )

        snr_window = (
            module_settings.p_wave.snr_window.noise,
            module_settings.p_wave.snr_window.signal,
        )

        st_c = st.copy().composite()
        snrs_p, p_snr_picks = snr_picker(
            st_c, picks, snr_dt=search_window, snr_window=snr_window, filter="P"
        )
        t5 = time()
        logger.info("done picking P-wave in %0.3f seconds" % (t5 - t4))

        logger.info("picking S-waves")
        t6 = time()

        search_window = np.arange(
            module_settings.s_wave.search_window.start,
            module_settings.s_wave.search_window.end,
            module_settings.s_wave.search_window.resolution,
        )

        snr_window = (
            module_settings.s_wave.snr_window.noise,
            module_settings.s_wave.snr_window.signal,
        )

        snrs_s, s_snr_picks = snr_picker(
            st_c, picks, snr_dt=search_window, snr_window=snr_window, filter="S"
        )
        t7 = time()

        logger.info("done picking S-wave in %0.3f seconds" % (t7 - t6))

        snr_picks = p_snr_picks + s_snr_picks
        snrs = snrs_p + snrs_s

        snr_picks_filtered = [
            snr_pick
            for (snr_pick, snr) in zip(snr_picks, snrs)
            if snr > module_settings.snr_threshold
        ]

        snr_picks_filtered_list.append(snr_picks_filtered)
        snr_picks_len.append(len(snr_picks_filtered))

    # selecting the set of picks that contains the most picks
    logger.info('selecting the set of picks containing the most picks')
    index = np.argmax(snr_picks_len)
    snr_picks_filtered = snr_picks_filtered_list[index]
    origin_time_calculation_method = 'Interloc'
    if index == 0:
        origin_time_calculation_method = 'SPP'

    logger.info('Origin time yielding to the most picks is %s' %
                origin_time_calculation_method)

    logger.info('SSP: %d picks' %snr_picks_len[0])
    if len(snr_picks_len) > 1:
        logger.info('Interloc: %d picks' % snr_picks_len[1])

    logger.info("correcting bias in origin time")
    t0 = time()
    residuals = []
    for snr_pk in snr_picks_filtered:
        for pk in picks:
            if (pk.phase_hint == snr_pk.phase_hint) and (
                pk.waveform_id.station_code == snr_pk.waveform_id.station_code
            ):
                residuals.append(pk.time - snr_pk.time)



    ot_utc -= np.mean(residuals)

    biais = np.mean(residuals)
    residuals -= biais
    indices = np.nonzero(np.abs(residuals) < residual_tolerance)[0]
    snr_picks_filtered = [snr_picks_filtered[i] for i in indices]

    t1 = time()
    logger.info("done correcting bias in origin time in %0.3f" % (t1 - t0))
    logger.info("bias in origin time was %0.3f seconds" % biais)

    logger.info("creating arrivals")
    t8 = time()
    arrivals = app.create_arrivals_from_picks(snr_picks_filtered, loc, ot_utc)
    # import pdb; pdb.set_trace()

    t9 = time()
    logger.info("done creating arrivals in %0.3f seconds" % (t9 - t8))

    logger.info("creating new event or appending to existing event")
    t10 = time()

    t11 = time()

    logger.info("Origin time: %s" % ot_utc)
    logger.info("Total number of picks: %d" % len(arrivals))

    logger.info(
        "done creating new event or appending to existing event "
        "in %0.3f seconds" % (t11 - t10)
    )

    origin = Origin()
    origin.time = ot_utc
    origin.x = o_loc[0]
    origin.y = o_loc[1]
    origin.z = o_loc[2]
    origin.arrivals = arrivals
    origin.evaluation_mode = "automatic"
    origin.evaluation_status = "preliminary"
    origin.creation_info = CreationInfo(creation_time=UTCDateTime.now())
    origin.method_id = "PICKER_FOR_HOLDING_ARRIVALS"

    cat[0].picks += snr_picks_filtered
    cat[0].origins += [origin]
    cat[0].preferred_origin_id = origin.resource_id.id

    return cat, stream


__module_name__ = "picker"


def main():
    """
    Run the picking module
    """

    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
