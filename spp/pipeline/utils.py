from microquake.nlloc import NLL

from ..core.settings import settings


def prepare_velocities(app=None, module_settings=None):
    vp_grid, vs_grid = app.get_velocities()

    return {"vp_grid": vp_grid, "vs_grid": vs_grid}


def prepare_nll(app=None, module_settings=None):
    project_code = settings.PROJECT_CODE
    base_folder = settings.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()
    print(base_folder)
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
