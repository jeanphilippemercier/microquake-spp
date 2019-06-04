import os

from microquake.core.data.grid import read_grid
from microquake.simul.eik import ray_tracer

from ..core.settings import settings


def get_grid(station_code, phase, type='time'):
    """
    get a travel time grid for a given station and a given phase
    :param station_code: station code
    :param phase: Phase ('P' or 'S')
    :param type: type of grid ('time', 'take_off', 'azimuth')
    :return:
    """
    nll_dir = settings.nll_base
    f_tt = os.path.join(nll_dir, 'time', 'OT.%s.%s.%s.buf'
                        % (phase.upper(), station_code, type))
    tt_grid = read_grid(f_tt, format='NLLOC')

    return tt_grid


def get_grid_point(station_code, phase, location,
                   grid_coordinates=False, type='time'):
    """
    get value on a grid at a given point inside the grid
    :param station_code: Station code
    :param phase: Phase ('P' or 'S')
    :param location: point where the value is interpolated
    :param grid_coordinates: whether the location is expressed in grid
    coordinates or in model coordinates (default True)
    :param type: type of grid ('time', 'take_off', 'azimuth')
    :return:
    """

    tt = get_grid(station_code, phase, type=type)

    return tt.interpolate(location, grid_coordinate=grid_coordinates)[0]


def get_ray(station_code, phase, location, grid_coordinate=False):
    """
    return a ray for a given location - station pair for a given phase
    :param station_code: station code
    :param phase: phase ('P', 'S')
    :param location: start of the ray
    :param grid_coordinate: whether start is expressed in  grid
    coordinates or model coordinates (default False)
    :return:
    """
    travel_time = get_grid(station_code, phase, type='time')

    return ray_tracer(travel_time, location,
                      grid_coordinates=grid_coordinate)
