def prepare_velocities(app=None, module_settings=None):
    vp_grid, vs_grid = app.get_velocities()
    return {"vp_grid": vp_grid, "vs_grid": vs_grid}
