import os
from microquake.core import AttribDict
import toml
from microquake.core.data.grid import create, read_grid
from microquake.simul.eik import ray_tracer

class Grid(object):

    def __init__(self, toml_file=None):
        """

        :param toml_file: path to the TOML file containing the project
        parameter. If not set, the function will look for a file named
        settings.toml in the $SPP_CONFIG directory
        :param module_name: name of the module, the name must be coherent
        with a section in the config file.
        :param processing_flow: Name of the processing flow. This must
        correspond to a section in the config file
        :param processing_flow_name: initialize the processing flow by
        setting the processing step to 0.
        :return: None
        """

        self.config_dir = os.environ['SPP_CONFIG']
        self.common_dir = os.environ['SPP_COMMON']

        if toml_file is None:
            toml_file = os.path.join(self.config_dir, 'settings.toml')
        self.toml_file = toml_file

        self.settings = AttribDict(toml.load(self.toml_file))

    def get_velocities(self):
        """
        returns velocity models
        """
        if self.settings.grids.velocities.homogeneous:
            vp = create(**self.settings.grids)
            vp.data *= self.settings.grids.velocities.vp
            vp.resource_id = self.get_current_velocity_model_id('P')
            vs = create(**self.settings.grids)
            vs.data *= self.settings.grid.velocities.vs
            vs.resource_id = self.get_current_velocity_model_id('S')

        else:
            if self.settings.grids.velocities.source == 'local':
                format = self.settings.grids.velocities.format
                vp_path = os.path.join(self.common_dir,
                                       self.settings.grids.velocities.vp)
                vp = read_grid(vp_path, format=format)
                vp.resource_id = self.get_current_velocity_model_id('P')
                vs_path = os.path.join(self.common_dir,
                                       self.settings.grids.velocities.vs)
                vs = read_grid(vs_path, format=format)
                vs.resource_id = self.get_current_velocity_model_id('S')
            elif self.settings['grids.velocities.local']:
                # TODO: read the velocity grids from the server
                pass

        return vp, vs

    def get_grid(self, station_code, phase, type='time'):
        """
        get a travel time grid for a given station and a given phase
        :param station_code: station code
        :param phase: Phase ('P' or 'S')
        :param type: type of grid ('time', 'take_off', 'azimuth')
        :return:
        """
        from microquake.core.data.grid import read_grid
        import os

        common_dir = self.common_dir
        nll_dir = self.settings.nlloc.nll_base
        f_tt = os.path.join(common_dir, nll_dir, 'time', 'OT.%s.%s.%s.buf'
                            % (phase.upper(), station_code, type))
        tt_grid = read_grid(f_tt, format='NLLOC')

        return tt_grid

    def get_grid_point(self, station_code, phase, location,
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

        tt = self.get_grid(station_code, phase, type=type)
        return tt.interpolate(location, grid_coordinate=grid_coordinates)[0]

    def get_ray(self, station_code, phase, location, grid_coordinate=False):
        """
        return a ray for a given location - station pair for a given phase
        :param station_code: station code
        :param phase: phase ('P', 'S')
        :param location: start of the ray
        :param grid_coordinate: whether start is expressed in  grid
        coordinates or model coordinates (default False)
        :return:
        """

        travel_time = self.get_grid(station_code, phase, type='time')

        return ray_tracer(travel_time, location,
                         grid_coordinates=grid_coordinate)

    def get_current_velocity_model_id(self, phase='P'):
        """
        Return the velocity model ID for a specificed phase
        :param phase: phase (possible values 'P', 'S'
        :return: resource_identifier

        """
        common_dir = self.common_dir
        velocity_dir = self.settings.grids.velocities
        if phase.upper() == 'P':
            v_path = os.path.join(self.common_dir,
                                  self.settings.grids.velocities.vp) + '.rid'

        elif phase.upper() == 'S':
             v_path = os.path.join(self.common_dir,
                                   self.settings.grids.velocities.vs) + '.rid'

        with open(v_path) as ris:
            return ris.read()