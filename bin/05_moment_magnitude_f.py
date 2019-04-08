#!/usr/bin/env python3

import numpy as np

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

    if logger is None:
        logger = app.logger

    vp_grid = prepared_objects["vp_grid"]
    vs_grid = prepared_objects["vs_grid"]

    #params = module_settings.magnitude
    params = app.settings.magnitude

    density = params.density
    min_dist = params.min_dist
    use_free_surface_correction = params.use_free_surface_correction
    use_sdr_rad = params.smom.use_sdr_rad
    make_preferred = params.smom.make_preferred
    phase_list = params.smom.phase_list
    if not isinstance(phase_list, list):
        phase_list = [phase_list]

    if use_sdr_rad and cat.preferred_focal_mechanism() is None:
        logger.warn("use_sdr_rad=True but preferred focal mech = None --> Setting use_sdr_rad=False")
        use_sdr_rad = False

    cat_out = cat.copy()

    for i,event in enumerate(cat_out):

        ev_loc = event.preferred_origin().loc
        vp = vp_grid.interpolate(ev_loc)[0]
        vs = vs_grid.interpolate(ev_loc)[0]

        sdr = None
        if use_sdr_rad:
            focal_mech = event.preferred_focal_mechanism()
            if focal_mech is not None:
                nodal_plane = focal_mech.nodal_planes.nodal_plane_1
                strike = nodal_plane.strike
                dip = nodal_plane.dip
                rake = nodal_plane.rake
                sdr = (strike, dip, rake)
                logger.info("use_sdr_rad=True (s,d,r)=(%.1f,%.1f,%.1f)" % \
                            (strike, dip, rake))

        Mws = []
        station_mags = []

        for phase in phase_list:

            Mw, sta_mags = calc_magnitudes_from_lambda(
                                [event],
                                vp=vp,
                                vs=vs,
                                density=density,
                                P_or_S=phase,
                                use_smom=True,
                                use_sdr_rad=use_sdr_rad,
                                use_free_surface_correction=use_free_surface_correction,
                                sdr=sdr,
                                logger_in=logger)

            Mws.append(Mw)
            station_mags.extend(sta_mags)

            logger.info("Mw_%s=%.1f len(station_mags)=%d" % 
                        (phase, Mws[-1], len(station_mags)))

        Mw = np.mean(Mws)

        comment="Average of frequency-domain station moment magnitudes"
        if use_sdr_rad and sdr is not None:
            comment += " Use_sdr_rad: sdr=(%.1f,%.1f,%.1f)" % (sdr[0],sdr[1],sdr[2])

        if np.isnan(Mw):
            logger.warn("Mw is nan, cannot set on event")
            continue

        set_new_event_mag(event, station_mags, Mw, comment, make_preferred=make_preferred)


    return cat_out, stream


def prepare(app=None, module_settings=None):
    vp_grid, vs_grid = app.get_velocities()
    return {"vp_grid": vp_grid, "vs_grid": vs_grid}


__module_name__ = "magnitude_f"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
