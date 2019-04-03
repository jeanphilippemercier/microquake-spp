#!/usr/bin/env python3

import sys

import numpy as np

from microquake.core.stream import Stream
from microquake.waveform.amp_measures import measure_pick_amps
from microquake.waveform.transforms import rotate_to_ENZ, rotate_to_P_SV_SH
from spp.utils.cli import CLI


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):

    pulse_min_width = module_settings.pulse_min_width
    pulse_min_snr_P = module_settings.pulse_min_snr_P
    pulse_min_snr_S = module_settings.pulse_min_snr_S
    phase_list = module_settings.phase_list
    if not isinstance(phase_list, list):
        phase_list = [phase_list]

    st = stream.copy()
    cat_out = cat.copy()

    inventory = app.get_inventory()
    missing_responses = st.attach_response(inventory)
    for sta in missing_responses:
        logger.warn("Inventory: Missing response for sta:%s" % sta)


    # 1. Rotate traces to ENZ
    st_rot = rotate_to_ENZ(st, inventory)
    st = st_rot

    # 2. Rotate traces to P,SV,SH wrt event location
    st_new = rotate_to_P_SV_SH(st, cat_out)
    st = st_new

    # 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
    trP = [tr for tr in st if tr.stats.channel == 'P' or tr.stats.channel.upper() == 'Z']

    measure_pick_amps(Stream(traces=trP),
                      cat_out,
                      phase_list=phase_list,
                      pulse_min_width=pulse_min_width,
                      pulse_min_snr_P=pulse_min_snr_P,
                      pulse_min_snr_S=pulse_min_snr_S,
                      debug=False,
                      logger_in=logger)

    return cat_out, stream


__module_name__ = "measure_amplitudes"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
