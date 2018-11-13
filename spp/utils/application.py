import toml
import os
from microquake.core.util.attribdict import AttribDict
from microquake.core.data.grid import create, read_grid
from microquake.core.data.station import read_stations
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os


class Application(object):

    def __init__(self, toml_file=None):

        self.config_dir = os.environ['SPP_CONFIG']
        self.common_dir = os.environ['SPP_COMMON']

        if toml_file is None:
            toml_file = os.path.join(self.config_dir, 'settings.toml')
        self.toml_file = toml_file

        self.settings = AttribDict(toml.load(self.toml_file))

        # Appending the SPP_COMMON directory to nll_base

        if 'nlloc' in self.settings.__dict__.keys():
            self.settings.nlloc.nll_base = os.path.join(self.common_dir,
                                                        self.settings.nlloc.nll_base)

        if 'magnitude' in self.settings.__dict__.keys():
            if 'len_spectrum_exponent' in \
                    self.settings.magnitude.__dict__.keys():
                self.settings.magnitude.len_spectrum = 2 ** \
                self.settings.magnitude.len_spectrum_exponent

    @property
    def nll_tts_dir(self):
        return os.path.join(self.common_dir,
                         self.settings.nlloc.nll_base, 'time')

    def get_ttable_h5(self):
        from microquake.core.data import ttable
        fname = os.path.join(self.common_dir, self.settings.grids.hfile)
        return ttable.H5TTable(fname)

    def write_ttable_h5(self, fname=None):
        from microquake.core.data import ttable

        if fname is None:
            fname = self.settings.grids.hfile

        ttp = ttable.array_from_nll_grids(self.nll_tts_dir, 'P', prefix='OT')
        tts = ttable.array_from_nll_grids(self.nll_tts_dir, 'S', prefix='OT')
        fpath = os.path.join(self.common_dir, fname)
        ttable.write_h5(fpath, ttp, tdict2=tts)

    def get_stations(self):
        params = self.settings.sensors
        if params.source == 'local':
            fpath = os.path.join(self.common_dir, params.path)
            site = read_stations(fpath)
        elif self.settings.sensors.source == 'remote':
            pass

        return site

    def nll_velgrids(self):
        """
        Returns the information required by nll to initialize the nll object
        Returns:

        """

        out_dict = AttribDict()

        vp, vs = self.get_velocities()

        out_dict = AttribDict()
        out_dict.vp = self.settings.grids.velocities.vp
        out_dict.vs = self.settings.grids.velocities.vs
        out_dict.homogeneous = \
            self.settings.grids.velocities.homogeneous
        out_dict.grids = AttribDict()
        out_dict.grids.vp = vp
        out_dict.grids.vs = vs

        out_dict.index = 0

        return out_dict
        # reading the station information

    def nll_sensors(self):
        """
        Returns the information required by nll to initialize the nll object
        Returns: AttribDict

        """
        from numpy import array

        out_dict = AttribDict()

        site = self.get_stations()

        out_dict.site = site

        out_dict.name = array([station.code for station in site.stations()])
        out_dict.pos = array([station.loc for station in site.stations()])
        out_dict.key = '0'
        out_dict.index = 0

        return out_dict

    def nll_nll(self):
        return AttribDict(self.settings['nlloc'])

    def get_velocities(self):
        """
        returns velocity models
        """
        if self.settings.grids.velocities.homogeneous:
            vp = create(**self.settings.grids)
            vp.data *= self.settings.grids.velocities.vp
            vs = create(**self.settings.grids)
            vs.data *= self.settings.grid.velocities.vs

        else:
            if self.settings.grids.velocities.source == 'local':
                format = self.settings.grids.velocities.format
                vp_path = os.path.join(self.common_dir,
                                       self.settings.grids.velocities.vp)
                vp = read_grid(vp_path, format=format)
                vs_path = os.path.join(self.common_dir,
                                       self.settings.grids.velocities.vs)
                vs = read_grid(vs_path, format=format)
            elif self.settings['grids.velocities.local']:
                # TODO: read the velocity grids from the server
                pass

        return vp, vs

    def get_time_zone(self):
        """
        returns a time zone compatible object Handling of time zone is essential
        for seismic system as UTC time is used in the as the default time zone
        :return: a time zone object
        """

        tz_settings = self.settings.time_zone

        if tz_settings.type == "UTC_offset":
            from dateutil.tz import tzoffset
            offset = float(tz_settings.offset)    # offset in hours
            tz_code = tz_settings.time_zone_code  # code for the time zone
            tz = tzoffset(tz_code, offset * 3600)

        elif tz_settings.type  == "time_zone":
            import pytz
            valid_time_zones = pytz.all_timezones
            if tz_settings.time_zone_code not in valid_time_zones:
                # raise an exception
                pass
            else:
                tz = pytz.timezone(tz_settings.time_zone_code)

        return tz

    def get_travel_time_grid(self, sta_code, phase):
        """
        get a travel time grid for a given station and a given phase
        Args:
            station: station code
            phase: phase either "P" or "S"

        Returns:

        """
        from microquake.core.data.grid import read_grid
        import os
        
        common_dir = self.common_dir
        nll_dir = self.settings.nlloc.nll_base
        f_tt = os.path.join(common_dir, nll_dir, 'time', 'OT.%s.%s.time.buf'
                            % (phase.upper(), sta_code))
        tt_grid = read_grid(f_tt, format='NLLOC')
        # tt_grid.seed = station.loc

        return tt_grid

    def get_travel_time_grid_point(self, station, phase, location,
                                   grid_coordinates=True):
        """
        get the travel time
        :param stations: list of stations
        :param locations: event location triplet with (X, Y, Z) that
        can be converted to a numpy array, locations can be a vector of coordinates
        :param phase: Phase either P or S, if None both P and S travel time are
        extracted
        :param use_eikonal: If True read eikonal time grids; If False read NLLOC station time grids directly
        :param spark_context: a spark context for parallelization purpose
        :return: a pandas DataFrame
        """

        # import os
        # from pandas import DataFrame
        # from spp.time import get_time_zone

        # building spark keys
        # need to be parallelized but for now running in loops

        tt = self.get_travel_time_grid(station, phase)
        return tt.interpolate(location, grid_coordinate=grid_coordinates)

    def __get_console_handler(self):
        """
        get logger console handler
        Returns: console_handler

        """
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(self.settings.logging.log_format)
        console_handler.setFormatter(formatter)
        return console_handler

    def __get_file_handler(self, log_filename):
        """
        get logger file handler
        Returns: file handler

        """
        log_dir = self.settings.logging.log_directory
        formatter = logging.Formatter(self.settings.logging.log_format)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logger_file = os.path.join(log_dir, log_filename)
        file_handler = TimedRotatingFileHandler(logger_file,
                                                when='midnight')
        file_handler.setFormatter(formatter)
        return file_handler

    def get_logger(self, logger_name, log_filename):

        logger = logging.getLogger(logger_name)
        log_level = self.settings.logging.log_level

        if not len(logger.handlers):

            final_log_level = self.settings.logging.log_level
            if log_level is not None:
                final_log_level = log_level
            elif log_level is not None:
                final_log_level = log_level
            logger.setLevel(final_log_level)

            logger.addHandler(self.__get_console_handler())
            logger.addHandler(self.__get_file_handler(log_filename))
        return logger
