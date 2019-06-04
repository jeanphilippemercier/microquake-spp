"""
Predict hypocenter location
"""

from time import time

import numpy as np

from loguru import logger
from microquake.nlloc import NLL, calculate_uncertainty

from ..core.settings import settings


class Processor():
    def __init__(self, app, module_settings):
        self.app = app
        self.module_settings = module_settings
        self.vp_grid, self.vs_grid = app.get_velocities()

        project_code = settings.PROJECT_CODE
        base_folder = settings.nll_base
        gridpar = app.nll_velgrids()
        sensors = app.nll_sensors()

        logger.info("preparing NonLinLoc")
        self.nll = NLL(
            project_code,
            base_folder=base_folder,
            gridpar=gridpar,
            sensors=sensors,
            params=self.module_settings,
        )
        logger.info("done preparing NonLinLoc")

    def process(
        self,
        cat=None,
        stream=None,
    ):
        project_code = settings.PROJECT_CODE
        base_folder = settings.nll_base

        logger.info("running NonLinLoc")
        t0 = time()
        cat_out = self.nll.run_event(cat[0].copy())
        t1 = time()
        logger.info("done running NonLinLoc in %0.3f seconds" % (t1 - t0))

        if cat_out[0].preferred_origin():
            logger.info("preferred_origin exists from nlloc:")
        else:
            logger.info("No preferred_origin found")

        logger.info("calculating Uncertainty")
        t2 = time()
        picking_error = self.module_settings.picking_error
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

        # Fix the source angles (takeoff, azimuth) and add receiver angles (incidence, backazimuth)
        self.app.fix_arr_takeoff_and_azimuth(cat_out, self.vp_grid, self.vs_grid)

        # Just to reinforce that these are hypocentral distance in meters ... to be used by moment_mag calc
        # ie, obspy.arrival.distance = epicenteral distance in degrees

        if cat_out[0].preferred_origin():
            origin = cat_out[0].preferred_origin()

            for arr in origin.arrivals:
                arr.hypo_dist_in_m = arr.distance

        # cat_out.write("cat_nlloc.xml", format="QUAKEML")

        if cat_out[0].preferred_origin():
            logger.info("IMS   origin:  %s" % cat_out[0].origins[0].time)
            logger.info("NLLoc origin:  %s" % cat_out[0].preferred_origin().time)
            logger.info("IMS   location %s" % cat_out[0].origins[0].loc)
            logger.info("NLLoc location %s" % cat_out[0].preferred_origin().loc)
            dist = np.linalg.norm(
                cat_out[0].origins[0].loc - cat_out[0].preferred_origin().loc
            )
            logger.info("distance between two location %0.2f m" % dist)

        return cat_out, stream
