# Prepare the project directory
# must be used when updating the velocity models
# 1) prepare NLL directory and create the travel time table
# 2) convert the travel time table to H5F

from spp.utils.application import Application
from microquake.nlloc import NLL

if __name__ == '__main__':

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger(settings.nlloc.log_topic,
                            settings.nlloc.log_file_name)

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()
    params = app.settings.nlloc

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
    app.write_tttable_h5()