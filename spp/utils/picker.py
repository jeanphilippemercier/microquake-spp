def create_event(stream, event_location):
    """

    :param stream: a microquake.core.stream.Stream object containing event
    seismogram
    :param event_location: location of the event
    :return: catalog with one event, one origins and picks
    """

    import numpy as np
    from scipy.interpolate import interp1d
    from microquake.core.stream import Trace
    from microquake.core.event import (Catalog, Event, Origin, Pick, Arrival,
                                       WaveformStreamID)
    from obspy.realtime.signal import kurtosis
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
            tt = app.get_grid_point(station, phase=phase, location=phase)
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
            tt = app.get_grid_point(station, phase=phase, location=phase)
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
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id,
                                                   referred_object=origin)
    event.picks = picks
    #event.pick_method = 'predicted from get_travel_time_grid'

    catalog = Catalog()
    catalog.events = [event]

    return catalog