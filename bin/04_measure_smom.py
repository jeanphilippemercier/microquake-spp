#!/usr/bin/env python3

import sys

import numpy as np
from obspy.core.event.base import Comment

from microquake.core.stream import Stream
from microquake.waveform.smom_measure import measure_pick_smom
from spp.utils.cli import CLI


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):

    params = module_settings
    S_win_len = params.S_win_len
    pre_window_start_sec = params.pre_window_start_sec
    max_S_P_time = params.max_S_P_time
    use_fixed_fmin_fmax = params.use_fixed_fmin_fmax
    fmin = params.fmin
    fmax = params.fmax
    phase_list = params.phase_list
    if not isinstance(phase_list, list):
        phase_list = [phase_list]

    plot_fit = False
    #plot_fit = True

    st = stream.copy()
    cat_out = cat.copy()

    inventory = app.get_inventory()
    missing_responses = st.attach_response(inventory)
    for sta in missing_responses:
        logger.warn("Inventory: Missing response for sta:%s" % sta)

    for event in cat_out:
        origin = event.preferred_origin()
        synthetic_picks = app.synthetic_arrival_times(origin.loc, origin.time)

        for phase in phase_list:

            logger.info("Call measure_pick_smom for phase=[%s]" % phase)

            try:
                smom_dict, fc = measure_pick_smom(st, inventory, event,
                                                synthetic_picks,
                                                P_or_S=phase,
                                                fmin=fmin, fmax=fmax,
                                                use_fixed_fmin_fmax=use_fixed_fmin_fmax,
                                                plot_fit=plot_fit,
                                                debug_level=1,
                                                logger_in=logger)
            except Exception as e:
                logger.warn("Error in measure_pick_smom. Continuing to next phase in phase_list: \n %s", e)
                continue


            comment = Comment(text="corner_frequency_%s=%.2f measured for %s arrivals" % \
                              (phase, fc, phase))
            origin.comments.append(comment)

    return cat_out, stream


__module_name__ = "measure_smom"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
