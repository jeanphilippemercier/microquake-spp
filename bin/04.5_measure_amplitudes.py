#!/usr/bin/env python3

import sys

import numpy as np

from microquake.core.data.inventory import inv_station_list_to_dict
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
    st = stream.copy()
    cat_out = cat.copy()

    inventory = app.get_inventory()
    st.attach_response(inventory)

    sta_meta_dict = inv_station_list_to_dict(inventory)
    # 1. Rotate traces to ENZ
    st_rot = rotate_to_ENZ(st, inventory)
    st = st_rot

    # 2. Rotate traces to P,SV,SH wrt event location
    st_new = rotate_to_P_SV_SH(st, cat_out)
    st = st_new

    # 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
    trP = [tr for tr in st if tr.stats.channel == "P"]
    measure_pick_amps(
        Stream(traces=trP),
        cat_out,
        phase_list=["P"],
        min_pulse_width=0.00167,
        min_pulse_snr=5,
        debug=False,
    )

    return cat_out, stream


__module_name__ = "measure_amplitudes"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
