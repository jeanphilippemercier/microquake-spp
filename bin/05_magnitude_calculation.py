#!/usr/bin/env python3

from io import BytesIO
from time import time

from microquake.waveform import mag
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
    site = prepared_objects["site"]

    vp = vp_grid.interpolate(cat[0].preferred_origin().loc)[0]
    vs = vs_grid.interpolate(cat[0].preferred_origin().loc)[0]

    logger.info("calculating the moment magnitude")
    t3 = time()
    cat_out = mag.moment_magnitude(
        stream,
        cat[0],
        site,
        vp,
        vs,
        ttpath=module_settings.ttpath,
        only_triaxial=module_settings.only_triaxial,
        density=module_settings.density,
        min_dist=module_settings.min_dist,
        win_length=module_settings.win_length,
        len_spectrum=2 ** module_settings.len_spectrum_exponent,
        freq=module_settings.freq,
    )
    t4 = time()
    logger.info("done calculating the moment magnitude in %0.3f" % (t4 - t3))

    return cat_out, stream


def prepare(app=None, module_settings=None):
    vp_grid, vs_grid = app.get_velocities()
    site = app.get_stations()
    return {"vp_grid": vp_grid, "vs_grid": vs_grid, "site": site}


__module_name__ = "magnitude"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
