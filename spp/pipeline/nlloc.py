"""
Predict hypocenter location
"""

from time import time

import numpy as np

from loguru import logger
from microquake.nlloc import NLL, calculate_uncertainty

from ..core.grid import fix_arr_takeoff_and_azimuth
from ..core.nlloc import nll_sensors, nll_velgrids
from ..core.velocity import get_velocities
from .processing_unit import ProcessingUnit


class Processor(ProcessingUnit):
    def initializer(self):
        self.vp_grid, self.vs_grid = get_velocities()

        self.project_code = self.settings.PROJECT_CODE
        self.base_folder = self.settings.nll_base
        gridpar = nll_velgrids()
        sensors = nll_sensors()

        logger.info("preparing NonLinLoc")
        self.nll = NLL(
            self.project_code,
            base_folder=self.base_folder,
            gridpar=gridpar,
            sensors=sensors,
            params=self.params,
        )
        logger.info("done preparing NonLinLoc")

    def process(
        self,
        **kwargs
    ):
        """
        requires an event
        input: catalog

        montecarlo sampling, change the input get many locations
        many locations will form a point cloud

        change the x, y,z many times

        returns: x,y,z,time, uncertainty
        uncertainty measure the effect of errors of the measurements on the results
        measurement picks, results is the location

        returns point cloud
        """
        cat = kwargs["cat"]
        stream = kwargs["stream"]

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
        picking_error = self.params.picking_error
        origin_uncertainty = calculate_uncertainty(
            cat_out[0],
            self.base_folder,
            self.project_code,
            perturbation=5,
            pick_uncertainty=picking_error,
        )

        if cat_out[0].preferred_origin():
            cat_out[0].preferred_origin().origin_uncertainty = origin_uncertainty
            t3 = time()
            logger.info("done calculating uncertainty in %0.3f seconds" % (t3 - t2))

        # Fix the source angles (takeoff, azimuth) and add receiver angles (incidence, backazimuth)
        fix_arr_takeoff_and_azimuth(cat_out, self.vp_grid, self.vs_grid)

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
