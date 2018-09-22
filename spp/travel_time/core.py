
from IPython.core.debugger import Tracer

from microquake.core.data.grid import read_grid
from microquake.core import read_stations

def init_travel_time():
    """
    Calculate travel time grid if required
    :return: status
    """

    #from microquake.core import read_grid, read_stations
    import os
    from microquake.simul import eik

    common_dir = os.environ['SPP_COMMON']

    vp = read_grid(os.path.join(common_dir, 'Vp.pickle'))
    vs = read_grid(os.path.join(common_dir, 'Vs.pickle'))

    site = read_stations(common_dir + '/sensors.csv', has_header=True)

    if not os.path.exists(os.path.join(common_dir, 'tt_grids')):
        os.mkdir(os.path.join(common_dir, 'tt_grids'))


    for station in site.stations():
        seed = station.loc
        seed_label = station.code

        fname_p = os.path.join(common_dir, 'tt_grids', '%s_p.pickle' % station.code)
        fname_s = os.path.join(common_dir, 'tt_grids', '%s_s.pickle' % station.code)
        fname_az_p = os.path.join(common_dir, 'tt_grids', '%s_azimuth_p.pickle' % station.code)
        fname_to_p = os.path.join(common_dir, 'tt_grids', '%s_takeoff_p.pickle' % station.code)
        fname_az_s = os.path.join(common_dir, 'tt_grids', '%s_azimuth_s.pickle' % station.code)
        fname_to_s = os.path.join(common_dir, 'tt_grids', '%s_takeoff_s.pickle' % station.code)

        if os.path.exists(fname_p):
            ttp = read_grid(fname_p)
            if (ttp.seed != station.loc).any():
                ttp = eik.eikonal_solver(vp, seed, seed_label)
                ttp.write(fname_p, format='PICKLE')
                (ttp_azimuth, ttp_takeoff) = eik.angles(ttp)
                ttp_azimuth.write(fname_az_p)
                ttp_takeoff.write(fname_to_p)

        else:
            ttp = eik.eikonal_solver(vp, seed, seed_label)
            ttp.write(fname_p, format='PICKLE')
            (ttp_azimuth, ttp_takeoff) = eik.angles(ttp)
            ttp_azimuth.write(fname_az_p)
            ttp_takeoff.write(fname_to_p)

        if os.path.exists(fname_s):
            tts = read_grid(fname_s)
            if (tts.seed != station.loc).any():
                tts = eik.eikonal_solver(vs, seed, seed_label)
                (tts_azimuth, tts_takeoff) = eik.angles(tts)
                tts.write(fname_s, format='PICKLE')
                tts_azimuth.write(fname_az_s)
                tts_takeoff.write(fname_to_s)

        else:
            tts = eik.eikonal_solver(vs, seed, seed_label)
            tts.write(fname_s, format='PICKLE')
            (tts_azimuth, tts_takeoff) = eik.angles(tts)
            tts_azimuth.write(fname_az_s)
            tts_takeoff.write(fname_to_s)


    return 1


def __get_grid_value_single_station(station_phase, location, use_eikonal=False):
    """
    Get interpolated grid values for a single station and phase
    :param station_phase: a tuple containing the station id and the phase
    :param location: location in absolute coordinates, not grid coordinates
    :return: array containing the travel times for the locations
    """

    import os
    from spp.utils import get_stations
    common_dir = os.environ['SPP_COMMON']

    station = station_phase[0]
    phase = station_phase[1].lower()

    if use_eikonal:
        f_tt = os.path.join(common_dir, 'tt_grids', '%s_%s.pickle' % (station, phase))
        tt_grid = read_grid(f_tt, format='PICKLE')

    else:
        f_tt = os.path.join(common_dir, 'NLL/time', 'OT.%s.%s.time.buf' % (phase.upper(), station))
        tt_grid = read_grid(f_tt, format='NLLOC')

    tt = tt_grid.interpolate(location, grid_coordinate=False)

    return tt


def get_travel_time_grid_point(station, location, phase, use_eikonal=False):
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

    from microquake.core import UTCDateTime
    from microquake.spark import mq_map
    import os
    from pandas import DataFrame
    from spp.time import get_time_zone

    # building spark keys
    # need to be parallelized but for now running in loops

    station_phase = (station, phase)
    tt = __get_grid_value_single_station(station_phase, location, use_eikonal)


    return tt[0]


def get_travel_time_grid(station, phase):

    import os
    from spp.utils import get_stations
    common_dir = os.environ['SPP_COMMON']

    site = get_stations()
    station = site.select(station=station)[0]
    f_tt = os.path.join(common_dir, 'NLL/time', 'OT.%s.%s.time.buf' % (phase.upper(), station))
    tt_grid = read_grid(f_tt, format='NLLOC')
    tt_grid.seed = station.loc

    return tt_grid


def create_event(stream, event_location):
    """

    :param stream: a microquake.core.stream.Stream object containing event
    seismogram
    :param event_location: location of the event
    :return: catalog with one event, one origins and picks
    """

    import matplotlib.pyplot as plt

    import numpy as np
    from scipy.interpolate import interp1d
    from microquake.core.stream import Trace
    from microquake.core.event import (Catalog, Event, Origin, Pick, Arrival,
                                       WaveformStreamID)
    from microquake.realtime.signal import kurtosis
    from microquake.core import UTCDateTime

    starttimes = []
    endtimes = []
    sampling_rates = []
    for trace in stream:
        starttimes.append(trace.stats.starttime.datetime)
        endtimes.append(trace.stats.endtime.datetime)
        sampling_rates.append(trace.stats.sampling_rate)

    min_starttime = UTCDateTime(np.min(starttimes)) - 1.0
    max_endtime = UTCDateTime(np.max(endtimes))
    max_sampling_rate = np.max(sampling_rates)


    stations = np.unique([tr.stats.station for tr in stream])


    shifted_traces = []
    npts = np.int((max_endtime - min_starttime) * max_sampling_rate)
    t_i = np.arange(0, npts) / max_sampling_rate

    for phase in ["p", "s"]:
        for trace in stream.composite():
            station = trace.stats.station
            tt = get_travel_time_grid_point(station, event_location, phase=phase)
            trace.stats.starttime = trace.stats.starttime - tt
            data = trace.data
            data /= np.max(np.abs(data))
            sr = trace.stats.sampling_rate
            startsamp = int((trace.stats.starttime - min_starttime) *
                        trace.stats.sampling_rate)
            endsamp = startsamp + trace.stats.npts
            t = np.arange(startsamp, endsamp) / sr
            f = interp1d(t, data, bounds_error=False, fill_value=0)

            shifted_traces.append(f(t_i))

            # tr.plot()
            # plt.clf()
            # plt.plot(f(t_i))
            # plt.show()
            # input()

    shifted_traces = np.array(shifted_traces)
    w_len_sec = 50e-3
    w_len_samp = int(w_len_sec * max_sampling_rate)

    stacked_trace = np.sum(np.array(shifted_traces)**2, axis=0)
    stacked_trace = stacked_trace / np.max(np.abs(stacked_trace))
    #
    i_max = np.argmax(np.sum(np.array(shifted_traces)**2, axis=0))

    if i_max - w_len_samp < 0:
        pass
        # return

    stacked_tr = Trace()
    stacked_tr.data = stacked_trace
    stacked_tr.stats.starttime = min_starttime
    stacked_tr.stats.sampling_rate = max_sampling_rate

    k = kurtosis(stacked_tr, win=30e-3)
    diff_k = np.diff(k)

    o_i = np.argmax(np.abs(diff_k[i_max - w_len_samp: i_max + w_len_samp])) + \
          i_max - w_len_samp

    origin_time = min_starttime + o_i / max_sampling_rate
    # Tracer()()

    origin = Origin()
    origin.x = event_location[0]
    origin.y = event_location[1]
    origin.z = event_location[2]

    origin.time = origin_time
    origin.evaluation_mode = 'automatic'
    origin.evaluation_status = 'preliminary'
    origin.method = 'spp.travel_time.core.create_event'

    picks = []
    for phase in ['p', 's']:
        for station in stations:
            pk = Pick()
            tt = get_travel_time_grid_point(station, event_location, phase=phase)
            pk.phase_hint = phase.upper()
            pk.time = origin_time + tt
            pk.evaluation_mode = "automatic"
            pk.evaluation_status = "preliminary"
            pk.method = 'theoretical prediction from get_travel_time_grid, use_eikonal=False'

            waveform_id = WaveformStreamID()
            waveform_id.channel_code = ""
            waveform_id.station_code = station

            pk.waveform_id = waveform_id
            picks.append(pk)

    event = Event()
    event.origins = [origin]
    event.picks = picks
    #event.pick_method = 'predicted from get_travel_time_grid'

    catalog = Catalog()
    catalog.events = [event]

    return catalog




