#!/usr/bin/env python3

from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.waveform.mag_new import (calc_magnitudes_from_lambda,
                                         set_new_event_mag)
from spp.utils.cli import CLI


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):
    vp_grid = prepared_objects["vp_grid"]
    vs_grid = prepared_objects["vs_grid"]

    cat_out = cat.copy()

    comment = "Average of time-domain P moment magnitudes"
    if module_settings.use_smom:
        comment = "Average of frequency-domain P moment magnitudes"

    for i, event in enumerate(cat_out):

        ev_loc = event.preferred_origin().loc
        vp = vp_grid.interpolate(ev_loc)[0]
        vs = vs_grid.interpolate(ev_loc)[0]

        Mw_P, station_mags_P = calc_magnitudes_from_lambda(
            cat_out,
            vp=vp,
            vs=vs,
            density=module_settings.density,
            P_or_S=module_settings.P_or_S,
            use_smom=module_settings.use_smom,
            logger = app.logger
        )
        Mw = Mw_P
        if len(station_mags_P) == 0:
            continue
        station_mags = station_mags_P
        set_new_event_mag(event, station_mags, Mw, comment)

    return cat_out, stream


def prepare(app=None, module_settings=None):
    vp_grid, vs_grid = app.get_velocities()
    return {"vp_grid": vp_grid, "vs_grid": vs_grid}


__module_name__ = "magnitude"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
