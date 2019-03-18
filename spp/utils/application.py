import logging
import os
import sys
from abc import abstractmethod
from io import BytesIO
from logging.handlers import TimedRotatingFileHandler
from time import time

import matplotlib.pyplot as plt
import numpy as np
import toml

from microquake.core import read_events
from microquake.core.data.grid import create, read_grid
from microquake.core.data.inventory import Inventory, load_inventory
from microquake.core.data.station import read_stations
from microquake.core.util.attribdict import AttribDict
from microquake.io import msgpack


class Application(object):

    def __init__(self, toml_file=None, module_name=None,
                 processing_flow_name='automatic'):
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

        self.__module_name__ = module_name
        if self.__module_name__ is None:
            #print("No module name, application cannot initialise")
            pass

        self.config_dir = os.environ['SPP_CONFIG']
        self.common_dir = os.environ['SPP_COMMON']

        if toml_file is None:
            toml_file = os.path.join(self.config_dir, 'settings.toml')
        self.toml_file = toml_file

        self.settings = AttribDict(toml.load(self.toml_file))

        processing_flow = self.settings.processing_flow[processing_flow_name]
        self.trigger_data_name = processing_flow.trigger_data_name
        self.dataset = processing_flow.dataset
        self.processing_flow_steps = processing_flow.steps

        self.inventory = None

        self.logger = None

        # Appending the SPP_COMMON directory to nll_base

        if 'nlloc' in self.settings.__dict__.keys():
            self.settings.nlloc.nll_base = os.path.join(self.common_dir,
                                                        self.settings.nlloc.nll_base)

        if 'magnitude' in self.settings.__dict__.keys():
            if 'len_spectrum_exponent' in \
                    self.settings.magnitude.__dict__.keys():
                self.settings.magnitude.len_spectrum = 2 ** \
                self.settings.magnitude.len_spectrum_exponent

        self.logger = self.get_logger('application', './application.log')
        if self.__module_name__ and self.__module_name__ in self.settings:
            self.logger = self.get_logger(self.settings[
                                            self.__module_name__].log_topic,
                            self.settings[self.__module_name__].log_file_name)


    def get_consumer_topic(self, processing_flow, dataset, module_name, trigger_data_name, input_data_name=None):
        if input_data_name:
            return self.get_topic(dataset, input_data_name)

        if module_name == 'chain':
            return self.get_topic(dataset, trigger_data_name)

        if len(processing_flow) == 0:
            raise ValueError("Empty processing_flow, cannot determine consumer topic")
        processing_step = -1
        for i, flow_step in enumerate(processing_flow):
            if module_name in flow_step:
                processing_step = i
                break
        if self.get_output_data_name(module_name) == trigger_data_name:
            # This module is triggering the processing flow. It is not consuming on any topics.
            return ""
        if processing_step == -1:
            raise ValueError("Module {} does not exist in processing_flow, cannot determine consumer topic".format(module_name) )
        if processing_step == 0:
            # The first module always consumes from the triggering
            return self.get_topic(dataset, trigger_data_name)
        if len(processing_flow) < 2:
            raise ValueError("Processing flow is malformed and only has one step {}".format(processing_flow))
        input_module_name = processing_flow[processing_step - 1][0]
        input_data_name = self.get_output_data_name(input_module_name)
        return self.get_topic(dataset, input_data_name)


    def get_producer_topic(self, dataset, module_name):
        return self.get_topic(dataset, self.get_output_data_name(module_name))


    def get_topic(self, dataset, data_name):
        return "seismic_processing.{}.{}".format(dataset, data_name)


    def get_output_data_name(self, module_name):
        if module_name in self.settings:
            return self.settings[module_name].output_data_name
        else:
            return ""


    @property
    def nll_tts_dir(self):
        """
        returns the path where the travel time grids are stored
        :return: path
        """
        return os.path.join(self.common_dir,
                         self.settings.nlloc.nll_base, 'time')

    def get_ttable_h5(self):
        from microquake.core.data import ttable
        fname = os.path.join(self.common_dir,
                             self.settings.grids.travel_time_h5.fname)
        return ttable.H5TTable(fname)

    def write_ttable_h5(self, fname=None):
        from microquake.core.data import ttable

        if fname is None:
            fname = self.settings.grids.travel_time_h5.fname

        ttp = ttable.array_from_nll_grids(self.nll_tts_dir, 'P', prefix='OT')
        tts = ttable.array_from_nll_grids(self.nll_tts_dir, 'S', prefix='OT')
        fpath = os.path.join(self.common_dir, fname)
        ttable.write_h5(fpath, ttp, tdict2=tts)

    def get_inventory(self):
        params = self.settings.sensors

        if self.inventory is None:

            if params.source == 'local':
                # MTH: let's read in the stationxml directly for now!
                fpath = os.path.join(self.common_dir, params.stationXML)
                self.inventory = Inventory.load_from_xml(fpath)
                #fpath = os.path.join(self.common_dir, params.path)
                #self.inventory = load_inventory(fpath, format='CSV')
                if self.logger:
                    self.logger.info("Application: Load Inventory from:[%s]" % fpath)

            elif self.settings.sensors.source == 'remote':
                pass
        #else:
            #print("app.get_inventory: INVENTORY FILE ALREADY LOADED")

        return self.inventory

    def get_stations(self):

        params = self.settings.sensors
        if params.source == 'local':
            fpath = os.path.join(self.common_dir, params.path)
            site = read_stations(fpath, format='CSV')
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

        inventory = self.get_inventory()
        stations  = inventory.stations()
        out_dict.name = array([station.code for station in stations])
        out_dict.pos  = array([station.loc for station in stations])
        out_dict.site = "THIS IS NOT SET"

        '''
        site = self.get_stations()
        out_dict.site = site
        out_dict.name = array([station.code for station in site.stations()])
        out_dict.pos = array([station.loc for station in site.stations()])
        '''
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

        elif tz_settings.type == "time_zone":
            import pytz
            valid_time_zones = pytz.all_timezones
            if tz_settings.time_zone_code not in valid_time_zones:
                # raise an exception
                pass
            else:
                tz = pytz.timezone(tz_settings.time_zone_code)

        return tz

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
        from microquake.simul.eik import ray_tracer

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

        if len(logger.handlers) == 0:

            final_log_level = self.settings.logging.log_level
            if log_level is not None:
                final_log_level = log_level
            elif log_level is not None:
                final_log_level = log_level
            logger.setLevel(final_log_level)

            logger.addHandler(self.__get_console_handler())
            logger.addHandler(self.__get_file_handler(log_filename))

        # Disable the extra console logging:
        logger.propagate = False

        return logger

    def synthetic_arrival_times(self, event_location, origin_time):
        """
        calculate synthetic arrival time for all the station and returns a
        list of microquake.core.event.Pick object
        :param event_location: event location
        :param origin_time: event origin time
        :return: list of microquake.core.event.Pick
        """

        from microquake.core.event import WaveformStreamID, Pick
        from numpy.linalg import norm

        picks = []

        #stations = self.get_stations().stations()
        #site = self.get_stations()

        inventory = self.get_inventory()
        stations  = inventory.stations()

        for phase in ['P', 'S']:
            for station in stations:
                #station = station.code
                #st_loc = site.select(station=station).stations()[0].loc

                st_loc   = station.loc

                dist = norm(st_loc - event_location)
                if (phase == 'S') and (dist < 100):
                    continue

                #at = origin_time + self.get_grid_point(station, phase,
                at = origin_time + self.get_grid_point(station.code, phase,
                                                       event_location,
                                                       grid_coordinates=False)

                wf_id = WaveformStreamID(
                    network_code=self.settings.project_code,
                    station_code=station.code)
                    #station_code=station)
                pk = Pick(time=at, method='predicted', phase_hint=phase,
                          evaluation_mode='automatic',
                          evaluation_status='preliminary', waveform_id=wf_id)

                picks.append(pk)

        return picks

    def estimate_origin_time(self, stream, event_location):
        """
        estimate the origin time given an estimate of the event location and
        a set of traces
        :param stream: a microquake.core.Stream object containing a series
        of traces
        :param event_location: event location (list, tuple or numpy array)
        :return: estimate of the origin time
        """
        from microquake.core import UTCDateTime
        from microquake.core import Trace
        from scipy.interpolate import interp1d
        from obspy.realtime.signal import kurtosis
        # from IPython.core.debugger import Tracer
        # import matplotlib.pyplot as plt

        start_times = []
        end_times = []
        sampling_rates = []
        stream = stream.detrend('demean')
        for trace in stream:
            start_times.append(trace.stats.starttime.datetime)
            end_times.append(trace.stats.endtime.datetime)
            sampling_rates.append(trace.stats.sampling_rate)

        min_starttime = UTCDateTime(np.min(start_times)) - 1.0
        max_endtime = UTCDateTime(np.max(end_times))
        max_sampling_rate = np.max(sampling_rates)

        shifted_traces = []
        npts = np.int((max_endtime - min_starttime) * max_sampling_rate)
        t_i = np.arange(0, npts) / max_sampling_rate

        for phase in ['P', 'S']:
            for trace in stream.composite():
                station = trace.stats.station
                tt = self.get_grid_point(station, phase, event_location)
                trace.stats.starttime = trace.stats.starttime - tt
                data = np.nan_to_num(trace.data)

                # dividing by the signal std yield stronger signal then
                # dividing by the max. Dividing by the max amplifies the
                # noisy traces as signal is more homogeneous on these traces
                data /= np.std(data)
                # data /= np.max(np.abs(data))
                sr = trace.stats.sampling_rate
                startsamp = int((trace.stats.starttime - min_starttime) *
                            trace.stats.sampling_rate)
                endsamp = startsamp + trace.stats.npts
                t = np.arange(startsamp, endsamp) / sr
                try:
                    f = interp1d(t, data, bounds_error=False, fill_value=0)
                except:
                    continue

                shifted_traces.append(np.nan_to_num(f(t_i)))

        shifted_traces = np.array(shifted_traces)

        w_len_sec = 50e-3
        w_len_samp = int(w_len_sec * max_sampling_rate)

        stacked_trace = np.sum(np.array(shifted_traces) ** 2, axis=0)
        stacked_trace /= np.max(np.abs(stacked_trace))
        #
        i_max = np.argmax(np.sum(np.array(shifted_traces) ** 2, axis=0))

        if i_max - w_len_samp < 0:
            pass

        stacked_tr = Trace()
        stacked_tr.data = stacked_trace
        stacked_tr.stats.starttime = min_starttime
        stacked_tr.stats.sampling_rate = max_sampling_rate

        o_i = np.argmax(stacked_tr)
        #k = kurtosis(stacked_tr, win=30e-3)
        #diff_k = np.diff(k)

        # o_i = np.argmax(np.abs(diff_k[i_max - w_len_samp: i_max + w_len_samp])) + \
        #       i_max - w_len_samp

        origin_time = min_starttime + o_i / max_sampling_rate
        # Tracer()()
        return origin_time

    def create_arrivals_from_picks(self, picks, event_location, origin_time):
        """
        create a set of arrivals from a list of picks
        :param picks: list of microquake.core.event.Pick
        :param event_location: event location list, tuple or numpy array
        :param origin_time: event origin_time
        :return: list of microquake.core.event.Arrival
        """
        from microquake.core.event import Arrival
        from IPython.core.debugger import Tracer

        #print("create_arrival_from_picks: event_location:<%.1f, %.1f, %.1f>" % \
              #(event_location[0], event_location[1], event_location[2]))

        arrivals = []
        for pick in picks:
            station_code = pick.waveform_id.station_code

            arrival = Arrival()
            arrival.phase = pick.phase_hint
            phase = pick.phase_hint

            ray = self.get_ray(station_code, phase, event_location)
            arrival.distance = ray.length()

            # TODO: MTH: Gotta think about how to store the ray points. Obspy will not handle
            #       a list in the extra dict, so you won't be able to do something like event.copy() later
            #arrival.ray = list(ray.nodes)
            #for node in ray.nodes:
                #print(node)

            #xoff = ray.nodes[-2][0] - ray.nodes[-1][0]
            #yoff = ray.nodes[-2][1] - ray.nodes[-1][1]
            #zoff = ray.nodes[-2][2] - ray.nodes[-1][2]
            #baz = np.arctan2(xoff,yoff)
            #if baz < 0:
                #baz += 2.*np.pi

            #pick.backazimuth = baz*180./np.pi

            predicted_tt = self.get_grid_point(station_code, phase,
                                               event_location)
            predicted_at = origin_time + predicted_tt
            arrival.time_residual = pick.time - predicted_at
            arrival.takeoff_angle = self.get_grid_point(station_code, phase,
                                           event_location, type='take_off')
            arrival.azimuth = self.get_grid_point(station_code, phase,
                                          event_location, type='azimuth')
            #print("create arrival: type(arrival)=%s type(takeoff_angle)=%s type(azimuth)=%s" % \
                  #(type(arrival), type(arrival.takeoff_angle), type(arrival.azimuth)))

            # MTH: arrival azimuth/takeoff should be in degrees - I'm pretty sure the grids
            #  store them in radians (?)
            arrival.azimuth *= 180./np.pi
            if arrival.azimuth < 0:
                arrival.azimuth += 360.
            arrival.takeoff_angle *= 180./np.pi

            arrival.pick_id = pick.resource_id.id
            arrival.earth_model_id = self.get_current_velocity_model_id(phase)
            arrivals.append(arrival)

        return arrivals


    def clean_waveform_stream(self, waveform_stream, stations_black_list):
        for trace in waveform_stream:
            if trace.stats.station in stations_black_list:
                waveform_stream.remove(trace)
        return waveform_stream


    def close(self):
        self.logger.info('closing application...')

    @abstractmethod
    def send_message(self, cat, stream, topic=None):
        """
        send message
        """
        self.logger.info('preparing data')
        msg = self.serialise_message(cat, stream)
        self.logger.info('done preparing data')
        return msg

    @abstractmethod
    def receive_message(self, msg_in, callback, **kwargs):
        """
        receive message
        """
        self.logger.info('unpacking data')
        t2 = time()
        cat, stream = self.deserialise_message(msg_in)
        t3 = time()
        self.logger.info('done unpacking data in %0.3f seconds' % (t3 - t2))

        if self.settings.sensors.black_list is not None:
            self.clean_waveform_stream(stream, self.settings.sensors.black_list)

        if not kwargs:
            cat_out, st_out = callback(cat=cat, stream=stream, logger=self.logger)
        else:
            cat_out, st_out = callback(cat=cat, stream=stream, logger=self.logger,
                               **kwargs)

        return cat_out, st_out


    def serialise_message(self, cat, stream):
        ev_io = BytesIO()
        if cat is not None:
            cat[0].write(ev_io, format='QUAKEML')
        return msgpack.pack([stream, ev_io.getvalue()])


    def deserialise_message(self, data):
        stream, quake_ml_bytes = msgpack.unpack(data)
        cat = read_events(BytesIO(quake_ml_bytes), format='QUAKEML')
        return cat, stream



def plot_nodes(sta_code, phase, nodes, event_location):
    x = []
    y = []
    z = []
    h = []
    print(event_location[0], event_location[1])
    for node in nodes:
        x.append(node[0] - event_location[0])
        y.append(node[1] - event_location[1])
        z.append(node[2] - event_location[2])
        h.append(np.sqrt(x[-1]*x[-1] + y[-1]*y[-1]))
        print(x[-1], y[-1])

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.set_title("Sta:%s [%s] ray nodes - ev_loc at 0,0" % (sta_code, phase))
    ax.set_xlabel('x offset = Easting')
    ax.set_xlabel('horiz offset')
    ax.set_ylabel('y offset = Northing')
    ax.set_ylabel('z offset wrt ev dep')
    #ax.plot(x, y, 'b')
    ax.plot(h, z, 'b')
    plt.show()
    print("sta:%s phase:%s node.x[-2]=%f node.x[-1]=%f" % (sta_code, phase, nodes[-2][0], nodes[-1][0]))
    print("sta:%s phase:%s node.y[-2]=%f node.y[-1]=%f" % (sta_code, phase, nodes[-2][1], nodes[-1][1]))

    print("N nodes:%d" % len(nodes))
