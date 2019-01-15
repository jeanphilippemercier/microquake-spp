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

    def __init__(self, toml_file=None, module_name=None,
                 processing_flow=None, init_processing_flow=False):
        """

        :param toml_file: path to the TOML file containing the project
        parameter. If not set, the function will look for a file named
        settings.toml in the $SPP_CONFIG directory
        :param module_name: name of the module, the name must be coherent
        with a section in the config file.
        :param processing_flow: Name of the processing flow. This must
        correspond to a section in the config file
        :param init_processing_flow: initialize the processing flow by
        setting the processing step to 0.
        :return: None
        """

        self.__module_name__ = module_name

        self.config_dir = os.environ['SPP_CONFIG']
        self.common_dir = os.environ['SPP_COMMON']

        if toml_file is None:
            toml_file = os.path.join(self.config_dir, 'settings.toml')
        self.toml_file = toml_file
        self.processing_flow = processing_flow
        if init_processing_flow:
            self.processing_step = 0
        else:
            self.processing_step = None

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

        elif tz_settings.type  == "time_zone":
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

    def init_redis(self):
        from redis import StrictRedis
        return StrictRedis(**self.settings.redis_db)

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
        stations = self.get_stations().stations()
        site = self.get_stations()
        for phase in ['P', 'S']:
            for station in stations:
                station = station.code
                st_loc = site.select(station=station).stations()[0].loc
                dist = norm(st_loc - event_location)
                if (phase == 'S') and (dist < 100):
                    continue

                at = origin_time + self.get_grid_point(station, phase,
                                                       event_location,
                                                       grid_coordinates=False)

                wf_id = WaveformStreamID(
                    network_code=self.settings.project_code,
                    station_code=station)
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
        import numpy as np
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

        arrivals = []
        for pick in picks:
            station_code = pick.waveform_id.station_code

            arrival = Arrival()
            arrival.phase = pick.phase_hint
            phase = pick.phase_hint

            ray = self.get_ray(station_code, phase, event_location)
            arrival.distance = ray.length()
            arrival.ray = list(ray.nodes)
            predicted_tt = self.get_grid_point(station_code, phase,
                                               event_location)
            predicted_at = origin_time + predicted_tt
            arrival.time_residual = pick.time - predicted_at
            arrival.takeoff_angle = self.get_grid_point(station_code, phase,
                                           event_location, type='take_off')
            arrival.azimuth = self.get_grid_point(station_code, phase,
                                          event_location, type='azimuth')
            arrival.pick_id = pick.resource_id.id
            arrival.earth_model_id = self.get_current_velocity_model_id(phase)
            arrivals.append(arrival)

        return arrivals

    def get_kafka_producer(self, logger=None):
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers':
                             self.settings.kafka.brokers},
                             logger=logger)
        return producer

    def get_kafka_consumer(self, logger=None):
        from kafka import KafkaConsumer
        return KafkaConsumer(self.settings[
                                 self.__module_name__].kafka_consumer_topic,
                             # group_id=self.settings.kafka.group_id,
                             bootstrap_servers=self.settings.kafka.brokers)
        # from confluent_kafka import Consumer
        # consumer = Consumer({'bootstrap.servers':
        #                       self.settings.kafka.brokers,
        #                      'group.id': self.settings.kafka.group_id,
        #                      'auto.offset.reset': 'earliest'},
        #                      logger=logger)

        return consumer

    def init_module(self):
        """
        Initialize processing module
        :return: None
        """
        if self.__module_name__ is None:
            return

        self.logger = self.get_logger(self.settings[
                                          self.__module_name__].log_topic,
                        self.settings[self.__module_name__].log_file_name)

        self.logger.info('setting up Kafka')
        self.producer = self.get_kafka_producer(logger=self.logger)
        self.consumer = self.get_kafka_consumer(logger=self.logger)

        consumer_topics = self.settings[
            self.__module_name__].kafka_consumer_topic
        if type(consumer_topics) is list:
            self.consumer.subscribe(consumer_topics)
        else:
            self.consumer.subscribe([consumer_topics])

        self.logger.info('done setting up Kafka')

        self.logger.info('init connection to redis')
        self.redis_conn = self.init_redis()
        self.logger.info('connection to redis database successfully initated')

    def send_message(self, cat, stream, topic=None):
        """
        send message to the next module
        :param cat: a microquake.core.event.Catalog object
        :param stream: a microquake.core.Stream object
        :param topic: Kafka topic to which the message will be sent
        :return: None

        Note that for this to work the processing_flow must be defined
        """
        from io import BytesIO
        from microquake.io import msgpack
        import uuid

        steps = self.settings.processing_flow[self.processing_flow].steps
        if self.processing_step == len(steps):
            self.info('End of processing flow! No more processing step, '
                      'exiting')
            return

        step_topics = steps[self.processing_step]

        self.logger.info('Preparing data')
        data_out = []
        if cat is not None:
            ev_io = BytesIO()
            data_out.append(self.processing_flow)
            data_out.append(self.processing_step + 1)
            cat[0].write(ev_io, format='QUAKEML')
            data_out.append(ev_io.getvalue())

        # if stream is not None:
        data_out.append(stream)

        msg_out = msgpack.pack(data_out)
        # timestamp_ms = int(cat[0].preferred_origin().time.timestamp * 1e3)
        redis_key = str(uuid.uuid4())
        self.logger.info('done preparing data')

        self.logger.info('sending data to Redis with redis key = %s' %redis_key)
        self.redis_conn.set(redis_key, msg_out,
                            ex=self.settings.redis_extra.ttl)
        self.logger.info('done sending data to Redis')

        self.logger.info('sending message to kafka')

        if (self.processing_flow is None) and (topic is None):
            self.logger.error('self.processing_flow is not set and topic is '
                              'not defined... exiting')
            raise AttributeError('either self.processing_flow or topic have '
                                 'to be defined')

        # from IPython.core.debugger import Tracer
        # Tracer()()
        if type(step_topics) is list:
            for topic in step_topics:
                self.producer.produce(topic, redis_key)

        else:
            topic = step_topics
            self.producer.produce(topic, redis_key)

        self.logger.info('done sending message to kafka on topic %s' % topic)

    def receive_message(self, msg_in, callback, **kwargs):
        """
        receive message
        :param callback: callback function signature must be as follows:
        def callback(cat=None, stream=None, extra_msg=None, logger=None,
        **kwargs)
        :param msg_in: message read from kafka
        :return: what callback function returns
        """
        from microquake.io import msgpack
        from time import time
        from microquake.core import read_events
        from io import BytesIO

        # reload settings allows for settings to be changed dynamically.
        self.settings = AttribDict(toml.load(self.toml_file))

        topic = self.settings[self.__module_name__].kafka_consumer_topic

        self.logger.info('awaiting for message on topic %s' % topic)

        redis_key = msg_in.value
        self.logger.info('getting data from Redis (key: %s)' % redis_key)
        t0 = time()
        data = msgpack.unpack(self.redis_conn.get(redis_key))
        t1 = time()
        self.logger.info('done getting data from Redis in %0.3f seconds' % (
            t1 - t0))

        self.logger.info('unpacking data')
        t2 = time()
        st = data[3]
        cat = read_events(BytesIO(data[2]), format='QUAKEML')
        self.processing_flow = data[0]
        self.processing_step = data[1]
        t3 = time()
        self.logger.info('done unpacking data in %0.3f seconds' % (t3 - t2))

        self.logger.info('on %s processing flow, step %d'
                         % (self.processing_flow, self.processing_step))

        if not kwargs:
            cat, st = callback(cat=cat, stream=st, logger=self.logger)

        else:
            cat, st = callback(cat=cat, stream=st, logger=self.logger,
                               **kwargs)

        self.logger.info('awaiting for message on topic %s' % topic)
        return cat, st





