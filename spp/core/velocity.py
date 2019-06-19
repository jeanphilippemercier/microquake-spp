from os import path

from microquake.core.data.grid import create, read_grid

from .settings import settings


def get_current_velocity_model_id(phase='P'):
    """
    Return the velocity model ID for a specificed phase
    :param phase: phase (possible values 'P', 'S'
    :return: resource_identifier

    """

    if phase.upper() == 'P':
        v_path = path.join(settings.common_dir,
                           settings.grids.velocities.vp) + '.rid'

    elif phase.upper() == 'S':
        v_path = path.join(settings.common_dir,
                           settings.grids.velocities.vs) + '.rid'

    with open(v_path) as ris:
        return ris.read()


def get_velocities():
    """
    returns velocity models
    """

    grids = settings.grids

    if grids.velocities.homogeneous:
        vp = create(**grids)
        vp.data *= grids.velocities.vp
        vp.resource_id = get_current_velocity_model_id('P')
        vs = create(**grids)
        vs.data *= grids.velocities.vs
        vs.resource_id = get_current_velocity_model_id('S')

    else:
        if grids.velocities.source == 'local':
            format = grids.velocities.format
            vp_path = path.join(settings.common_dir,
                                grids.velocities.vp)
            vp = read_grid(vp_path, format=format)
            vp.resource_id = get_current_velocity_model_id('P')
            vs_path = path.join(settings.common_dir,
                                grids.velocities.vs)
            vs = read_grid(vs_path, format=format)
            vs.resource_id = get_current_velocity_model_id('S')
        elif settings['grids.velocities.local']:
            # TODO: read the velocity grids from the server
            pass

    return vp, vs
