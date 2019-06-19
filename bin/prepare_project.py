# Prepare the project directory
# must be used when updating the velocity models
# 1) prepare NLL directory and create the travel time table
# 2) convert the travel time table to H5F

from loguru import logger
from microquake.nlloc import NLL
from spp.core.settings import settings
from loguru import logger
from spp.core.nlloc import nll_sensors, nll_velgrids
from spp.core.hdf5 import write_ttable_h5

if __name__ == '__main__':

    project_code = settings.PROJECT_CODE
    base_folder = settings.nll_base
    gridpar = nll_velgrids()
    sensors = nll_sensors()
    params = settings.get('nlloc')

    # Preparing NonLinLoc
    logger.info('preparing NonLinLoc')
    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
              sensors=sensors, params=params)

    # creating NLL base project including travel time grids
    logger.info('Preparing NonLinLoc')
    nll.prepare()
    logger.info('Done preparing NonLinLoc')

    # creating H5 grid from NLL grids
    logger.info('writing h5 travel time table')
    write_ttable_h5()
