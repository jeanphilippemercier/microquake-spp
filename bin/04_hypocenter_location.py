#!/usr/bin/env python3
"""
Predict hypocenter location
"""

from time import time

import numpy as np

from lib_process import fix_arr_takeoff_and_azimuth
from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.nlloc import NLL, calculate_uncertainty
from spp.utils.cli import CLI


def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,
):
    nll = prepared_objects["nll"]
    project_code = app.settings.project_code
    base_folder = module_settings.nll_base

    logger.info("running NonLinLoc")
    t0 = time()
    cat_out = nll.run_event(cat[0].copy())
    t1 = time()
    logger.info("done running NonLinLoc in %0.3f seconds" % (t1 - t0))

    if cat_out[0].preferred_origin():
        logger.info("preferred_origin exists from nlloc:")
        logger.info(cat_out[0].preferred_origin())
        logger.info("Here comes the nlloc location:")
        logger.info(cat_out[0].preferred_origin().loc)
    else:
        logger.info("No preferred_origin found")

    logger.info("calculating Uncertainty")
    t2 = time()
    picking_error = module_settings.picking_error
    origin_uncertainty = calculate_uncertainty(
        cat_out[0],
        base_folder,
        project_code,
        perturbation=5,
        pick_uncertainty=picking_error,
    )
    if cat_out[0].preferred_origin():
        cat_out[0].preferred_origin().origin_uncertainty = origin_uncertainty
        t3 = time()
        logger.info("done calculating uncertainty in %0.3f seconds" % (t3 - t2))

    inventory = app.get_inventory()
    sta_meta_dict = inv_station_list_to_dict(inventory)

    # Fix nlloc origin.arrival angles:

    fix_arr_takeoff_and_azimuth(cat_out, sta_meta_dict, app=app)

    # Just to reinforce that these are hypocentral distance in meters ... to be used by moment_mag calc
    # ie, obspy.arrival.distance = epicenteral distance in degrees
    if cat_out[0].preferred_origin():
        origin = cat_out[0].preferred_origin()
        for arr in origin.arrivals:
            arr.hypo_dist_in_m = arr.distance

    cat_out.write("cat_nlloc.xml", format="QUAKEML")

    app.logger.info("IMS location %s" % cat_out[0].origins[0].loc)
    if cat_out[0].preferred_origin():
        app.logger.info("Interloc location %s" % cat_out[0].preferred_origin().loc)
        dist = np.linalg.norm(
            cat_out[0].origins[0].loc - cat_out[0].preferred_origin().loc
        )
        app.logger.info("distance between two location %0.2f m" % dist)

    return cat_out, stream


def prepare(app=None, module_settings=None):
    project_code = app.settings.project_code
    base_folder = module_settings.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()

    app.logger.info("preparing NonLinLoc")
    nll = NLL(
        project_code,
        base_folder=base_folder,
        gridpar=gridpar,
        sensors=sensors,
        params=module_settings,
    )
    app.logger.info("done preparing NonLinLoc")

    return {"nll": nll}


__module_name__ = "nlloc"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
