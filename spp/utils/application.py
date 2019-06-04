import logging
import os
import sys
from abc import abstractmethod
from io import BytesIO
from logging.handlers import TimedRotatingFileHandler
from time import time

import matplotlib.pyplot as plt
import numpy as np

from microquake.core import read_events
from microquake.core.data.grid import create, read_grid
from microquake.core.data.inventory import Inventory, load_inventory
from microquake.core.data.station import read_stations
from microquake.core.util.attribdict import AttribDict
from microquake.io import msgpack

from ..core.grid import get_grid, get_ray, get_grid_point
from ..core.settings import settings

from loguru import logger


class Application(object):

    def __init__(self, toml_file=None, module_name=None,
                 processing_flow_name=None):
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
        settings.load(toml_file)

        self.settings = settings

        self.__module_name__ = module_name

        if self.__module_name__ is None:
            #print("No module name, application cannot initialise")
            pass

        if processing_flow_name:
            processing_flow = self.settings.get('processing_flow')[processing_flow_name]
            self.trigger_data_name = processing_flow.trigger_data_name
            self.dataset = processing_flow.dataset
            self.processing_flow_steps = processing_flow.steps

        self.inventory = None

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
            raise ValueError(
                "Module {} does not exist in processing_flow, cannot determine consumer topic".format(module_name))

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
        if self.settings.get(module_name):
            return self.settings.get(module_name).output_data_name
        else:
            return ""

    @property
    def nll_tts_dir(self):
        """
        returns the path where the travel time grids are stored
        :return: path
        """

        return os.path.join(self.settings.nll_base, 'time')

    def get_ttable_h5(self):
        from microquake.core.data import ttable
        fname = os.path.join(settings.common_dir,
                             settings.grids.travel_time_h5.fname)

        return ttable.H5TTable(fname)

    def write_ttable_h5(self, fname=None):
        from microquake.core.data import ttable

        if fname is None:
            fname = settings.grids.travel_time_h5.fname

        ttp = ttable.array_from_nll_grids(self.nll_tts_dir, 'P', prefix='OT')
        tts = ttable.array_from_nll_grids(self.nll_tts_dir, 'S', prefix='OT')
        fpath = os.path.join(settings.common_dir, fname)
        ttable.write_h5(fpath, ttp, tdict2=tts)

    def get_inventory(self):
        params = settings.get('sensors')

        if self.inventory is None:

            if params.source == 'local':
                # MTH: let's read in the stationxml directly for now!
                fpath = os.path.join(settings.common_dir, params.stationXML)
                self.inventory = Inventory.load_from_xml(fpath)
                #fpath = os.path.join(settings.common_dir, params.path)
                #self.inventory = load_inventory(fpath, format='CSV')

                logger.info("Application: Load Inventory from:[%s]" % fpath)

            elif settings.get('sensors').source == 'remote':
                pass
        # else:
            #print("app.get_inventory: INVENTORY FILE ALREADY LOADED")

        return self.inventory

    def get_stations(self):

        params = self.settings.get('sensors')

        if params.source == 'local':
            fpath = os.path.join(settings.common_dir, params.path)
            site = read_stations(fpath, format='CSV')
        elif self.settings.get('sensors').source == 'remote':
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
        out_dict.vp = settings.grids.velocities.vp
        out_dict.vs = settings.grids.velocities.vs
        out_dict.homogeneous = \
            settings.grids.velocities.homogeneous
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
        stations = inventory.stations()
        out_dict.name = array([station.code for station in stations])
        out_dict.pos = array([station.loc for station in stations])
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
        return AttribDict(self.settings.get('nlloc'))

    def get_velocities(self):
        """
        returns velocity models
        """

        grids = settings.grids

        if grids.velocities.homogeneous:
            vp = create(**grids)
            vp.data *= grids.velocities.vp
            vp.resource_id = self.get_current_velocity_model_id('P')
            vs = create(**grids)
            vs.data *= grids.velocities.vs
            vs.resource_id = self.get_current_velocity_model_id('S')

        else:
            if grids.velocities.source == 'local':
                format = grids.velocities.format
                vp_path = os.path.join(settings.common_dir,
                                       grids.velocities.vp)
                vp = read_grid(vp_path, format=format)
                vp.resource_id = self.get_current_velocity_model_id('P')
                vs_path = os.path.join(settings.common_dir,
                                       grids.velocities.vs)
                vs = read_grid(vs_path, format=format)
                vs.resource_id = self.get_current_velocity_model_id('S')
            elif settings['grids.velocities.local']:
                # TODO: read the velocity grids from the server
                pass

        return vp, vs

    def get_time_zone(self):
        """
        returns a time zone compatible object Handling of time zone is essential
        for seismic system as UTC time is used in the as the default time zone
        :return: a time zone object
        """

        tz_settings = self.settings.get('time_zone')

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

    def get_current_velocity_model_id(self, phase='P'):
        """
        Return the velocity model ID for a specificed phase
        :param phase: phase (possible values 'P', 'S'
        :return: resource_identifier

        """
        if phase.upper() == 'P':
            v_path = os.path.join(settings.common_dir,
                                  settings.grids.velocities.vp) + '.rid'

        elif phase.upper() == 'S':
            v_path = os.path.join(settings.common_dir,
                                  settings.grids.velocities.vs) + '.rid'

        with open(v_path) as ris:
            return ris.read()

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

        # stations = self.get_stations().stations()
        # site = self.get_stations()

        inventory = self.get_inventory()
        stations = inventory.stations()

        for phase in ['P', 'S']:
            for station in stations:
                # station = station.code
                # st_loc = site.select(station=station).stations()[0].loc

                st_loc = station.loc

                dist = norm(st_loc - event_location)

                if (phase == 'S') and (dist < 100):
                    continue

                # at = origin_time + get_grid_point(station, phase,
                at = origin_time + get_grid_point(station.code, phase,
                                                       event_location,
                                                       grid_coordinates=False)

                wf_id = WaveformStreamID(
                    network_code=self.settings.get('project_code'),
                    station_code=station.code)
                # station_code=station)
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
                tt = get_grid_point(station, phase, event_location)
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

        # print("create_arrival_from_picks: event_location:<%.1f, %.1f, %.1f>" % \
        # (event_location[0], event_location[1], event_location[2]))

        arrivals = []

        for pick in picks:
            station_code = pick.waveform_id.station_code

            arrival = Arrival()
            arrival.phase = pick.phase_hint
            phase = pick.phase_hint

            ray = get_ray(station_code, phase, event_location)
            arrival.distance = ray.length()

            # TODO: MTH: Gotta think about how to store the ray points. Obspy will not handle
            #       a list in the extra dict, so you won't be able to do something like event.copy() later
            #arrival.ray = list(ray.nodes)
            # for node in ray.nodes:
            # print(node)

            #xoff = ray.nodes[-2][0] - ray.nodes[-1][0]
            #yoff = ray.nodes[-2][1] - ray.nodes[-1][1]
            #zoff = ray.nodes[-2][2] - ray.nodes[-1][2]
            #baz = np.arctan2(xoff,yoff)
            # if baz < 0:
            #baz += 2.*np.pi

            #pick.backazimuth = baz*180./np.pi

            predicted_tt = get_grid_point(station_code, phase,
                                               event_location)
            predicted_at = origin_time + predicted_tt
            arrival.time_residual = pick.time - predicted_at
            # print("create_arrivals: sta:%3s pha:%s pick.time:%s

            arrival.takeoff_angle = get_grid_point(station_code, phase,
                                                        event_location, type='take_off')
            arrival.azimuth = get_grid_point(station_code, phase,
                                                  event_location, type='azimuth')
            # print("create arrival: type(arrival)=%s type(takeoff_angle)=%s type(azimuth)=%s" % \
            # (type(arrival), type(arrival.takeoff_angle), type(arrival.azimuth)))

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

    def fix_arr_takeoff_and_azimuth(self, cat, vp_grid, vs_grid):
        """
        Currently NLLoc is *not* calculating the takeoff angles at the source.
        These default to -1 so that when microquake.nlloc reads last.hyp it
        returns -1 for these values.

        Here we re-create the arrivals from the picks & the NLLoc location
        so that it populates the takeoff and azimuth angles.
        Also, we add the relevant angles at the receiver (backazimuth and incidence)
        to the arrivals.
        """

        for event in cat:
            origin = event.preferred_origin()
            ev_loc = origin.loc

            vp = vp_grid.interpolate(ev_loc)[0]
            vs = vs_grid.interpolate(ev_loc)[0]

            picks = []

            for arr in origin.arrivals:
                picks.append(arr.pick_id.get_referred_object())

            inventory = self.get_inventory()

    # MTH: create_arrivals_from_picks will create an entirely new set of arrivals (new resource_ids)
    #      it will set arr.distance (looks exactly same as nlloc's arr.distance)
    #      it will set arr.time_residual *** DIFFERS *** from arr.time_residual nlloc calcs/reads from last.hypo
    #      it will fix the missing azim/theta that nlloc set to -1
    #      it will drop nlloc arr.time_weight field

            arrivals = self.create_arrivals_from_picks(picks, ev_loc, origin.time)

    # Now set the receiver angles (backazimuth and incidence angle)

            for arr in arrivals:
                pk = arr.pick_id.get_referred_object()
                sta = pk.waveform_id.station_code
                pha = arr.phase

                st_loc = inventory.get_station(sta).loc

                xoff = ev_loc[0]-st_loc[0]
                yoff = ev_loc[1]-st_loc[1]
                zoff = np.abs(ev_loc[2]-st_loc[2])
                H = np.sqrt(xoff*xoff + yoff*yoff)
                alpha = np.arctan2(zoff, H)
                beta = np.pi/2. - alpha
                takeoff_straight = alpha * 180./np.pi + 90.
                inc_straight = beta * 180./np.pi

                if pha == 'P':
                    v = vp
                    v_grid = vp_grid
                elif pha == 'S':
                    v = vs
                    v_grid = vs_grid

                p = np.sin(arr.takeoff_angle*np.pi/180.) / v

                v_sta = v_grid.interpolate(st_loc)[0]

                inc_p = np.arcsin(p*v_sta) * 180./np.pi

                # I have the incidence angle now, need backazimuth so rotate to P,SV,SH
                back_azimuth = np.arctan2(xoff, yoff) * 180./np.pi

                if back_azimuth < 0:
                    back_azimuth += 360.

                arr.backazimuth = back_azimuth
                arr.inc_angle = inc_p

                '''
                print("%3s: [%s] takeoff:%6.2f [stx=%6.2f] inc_p:%.2f [inc_stx:%.2f] baz:%.1f [az:%.1f]" % \
                    (sta, arr.phase, arr.takeoff_angle, takeoff_straight, \
                    inc_p, inc_straight, back_azimuth, arr.azimuth))
                '''

            origin.arrivals = arrivals

        return

    def clean_waveform_stream(self, waveform_stream, stations_black_list):
        for trace in waveform_stream:
            if trace.stats.station in stations_black_list:
                waveform_stream.remove(trace)

        return waveform_stream

    def close(self):
        logger.info('closing application...')

    @abstractmethod
    def send_message(self, cat, stream, topic=None):
        """
        send message
        """
        logger.info('preparing data')
        msg = self.serialise_message(cat, stream)
        logger.info('done preparing data')

        return msg

    @abstractmethod
    def receive_message(self, msg_in, processor, **kwargs):
        """
        receive message
        """
        logger.info('unpacking data')
        t2 = time()
        cat, stream = self.deserialise_message(msg_in)
        t3 = time()
        logger.info('done unpacking data in %0.3f seconds' % (t3 - t2))

        (cat, stream) = self.clean_message((cat, stream))
        logger.info("processing event %s", str(cat[0].resource_id))

        if not kwargs:
            cat_out, st_out = processor.process(cat=cat, stream=stream)
        else:
            cat_out, st_out = processor.process(cat=cat, stream=stream,
                                       **kwargs)

        return cat_out, st_out

    def clean_message(self, msg_in):
        (catalog, waveform_stream) = msg_in

        if self.settings.get('sensors').black_list is not None:
            self.clean_waveform_stream(
                waveform_stream, self.settings.get('sensors').black_list
            )

        return (catalog, waveform_stream)

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
