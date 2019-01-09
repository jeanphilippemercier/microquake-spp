#!/usr/bin/env python3

from spp.utils.application import Application
from microquake.waveform.pick import snr_picker, sta_lta_picker
from microquake.waveform import pick as pppp
import numpy as np
from microquake.core.event import (Origin, CreationInfo)
from microquake.core import UTCDateTime
from IPython.core.debugger import Tracer
from importlib import reload
reload(pppp)

def picker(cat=None, stream=None, extra_msgs=None, logger=None, params=None,
           app=None):

    from time import time

    logger.info('calculating origin time')
    t0 = time()
    loc = cat[0].preferred_origin().loc
    ot_utc = app.estimate_origin_time(stream, loc)
    t1 = time()
    logger.info('done calculating origin time in %0.3f seconds' % (t1 - t0))


    logger.info('predicting picks')
    t2 = time()
    o_loc = cat[0].preferred_origin().loc
    picks = app.synthetic_arrival_times(o_loc, ot_utc)
    phase = np.array([pick.phase_hint for pick in picks])
    station = np.array([pick.waveform_id.station_code for pick in picks])
    pick_dict = {'phase':phase, 'station':station}
    t3 = time()
    logger.info('done predicting picks in %0.3f seconds' % (t3 - t2))

    freq_min = params.waveform_filter.frequency_min
    freq_max = params.waveform_filter.frequency_max

    st = stream.copy().detrend('demean')
    st = st.taper(max_percentage=0.1, max_length=0.01)
    st = st.filter('bandpass', freqmin=freq_min, freqmax=freq_max)


    logger.info('picking P-waves')
    t4 = time()
    search_window = np.arange(params.p_wave.search_window.start,
                              params.p_wave.search_window.end,
                              params.p_wave.search_window.resolution)

    snr_window = (params.p_wave.snr_window.noise,
                  params.p_wave.snr_window.signal)

    snrs_p, p_snr_picks = pppp.snr_picker(st, picks,
                                     snr_dt=search_window,
                                     snr_window=snr_window,  filter='P')

    # Measuring the difference between the predicted and picked arrival time

    residuals = []
    for p_snr_pick in p_snr_picks:
        index = np.nonzero((pick_dict['station'] ==
                     p_snr_pick.waveform_id.station_code) &
                     (pick_dict['phase'] == p_snr_pick.phase_hint))[0]
        if not index:
            continue
        pick = picks[index[0]]
        residuals.append(p_snr_pick.time - pick.time)

    dt = np.median(residuals)
    logger.info("median travel time residual %0.3f" % dt)

    # correcting for bias in the origin time estimation as captured by the
    # bias in the p-picks
    picks_2 = []
    for pick in picks:
        pick.time += dt
        picks_2.append(pick)

    picks = picks_2

    t5 = time()
    logger.info('done picking P-wave in %0.3f seconds' % (t5 - t4))

    logger.info('picking S-waves')
    t6 = time()

    search_window = np.arange(params.s_wave.search_window.start,
                              params.s_wave.search_window.end,
                              params.s_wave.search_window.resolution)

    snr_window = (params.s_wave.snr_window.noise,
                  params.s_wave.snr_window.signal)

    snrs_s, s_snr_picks = snr_picker(st, picks,
                                     snr_dt=search_window,
                                     snr_window=snr_window,
                                     filter='S')
    t7 = time()
    logger.info('done picking S-wave in %0.3f seconds' % (t7 - t6))

    snr_picks = p_snr_picks + s_snr_picks
    snrs = snrs_p + snrs_s

    snr_picks_filtered = [snr_pick for (snr_pick, snr)
                          in zip(snr_picks, snrs)
                          if snr > params.snr_threshold]

    phases_snr = np.array([pick.phase_hint for pick in snr_picks_filtered])
    stations_snr = np.array([pick.waveform_id.station_code for pick in
                             snr_picks_filtered])

    # check for incorrect phase
    snr_picks_tmp = []
    for station in np.unique(stations_snr):
        if station not in stations_snr:
            continue
        index = np.nonzero((stations_snr == station) &
                            (phases_snr == 'S'))[0]
        if not index:
            continue
        pick_s = snr_picks_filtered[index[0]]

        index = np.nonzero((stations_snr == station) &
                            (phases_snr == 'P'))[0]
        if not index:
            # setting up the pick to something (value not important)
            p_time = 0
        else:
            pick_p = snr_picks_filtered[index[0]]
            p_time = pick_p.time

        pred_p = picks[np.nonzero((pick_dict['station'] == station) &
                                  (pick_dict['phase'] == 'P'))[0][0]]
        pred_s = picks[np.nonzero((pick_dict['station'] == station) &
                                   (pick_dict['phase'] == 'S'))[0][0]]

        dt_p = np.abs(pick_s.time - pred_p.time)
        dt_s = np.abs(pick_s.time - pred_s.time)
        if (dt_p < dt_s) and (dt_p < params.residual_tolerance):
            pick_s.phase_hint = 'P'
            snr_picks_tmp.append(pick_s)
        elif np.abs(pick_s.time - p_time) < params.p_s_tolerance:
            snr_picks_tmp.append(pick_p)
        elif not isinstance(p_time, UTCDateTime):
            snr_picks_tmp.append(pick_s)
        else:
            snr_picks_tmp.append(pick_s)
            snr_picks_tmp.append(pick_p)

    # snr_picks_out = []
    # for snr_pick in snr_picks_tmp:
    #     for pick in picks:
    #         if ((snr_pick.phase_hint == pick.phase_hint) &
    #             (snr_pick.waveform_id.station_code ==
    #              pick.waveform_id.station_code)):
    #
    #             residuals.append(snr_pick.time - pick.time)
    #             residual = snr_pick.time - pick.time
    #             if residual < params.residual_tolerance:
    #                 snr_picks_out.append(snr_pick)

    logger.info('creating arrivals')
    t8 = time()
    arrivals = app.create_arrivals_from_picks(snr_picks_tmp, loc, ot_utc)
    t9 = time()
    logger.info('done creating arrivals in %0.3f seconds' % (t9 - t8))

    logger.info('creating new event or appending to existing event')
    t10 = time()

    t11 = time()

    logger.info('Origin time: %s' % ot_utc)


    # TODO: Need to check the event database for IMS event.
    # if an event exist, 1) get the event, 2) create a new origin with
    # information from interloc, 3) append picks to event, and 4) append
    # arrivals to the new origin

    origin = Origin()
    origin.time = ot_utc
    origin.x = o_loc[0]
    origin.y = o_loc[1]
    origin.z = o_loc[2]
    origin.arrivals = arrivals
    origin.evaluation_mode = 'automatic'
    origin.evaluation_status = 'preliminary'
    origin.creation_info = CreationInfo(creation_time=UTCDateTime.now())

    cat[0].picks += snr_picks_filtered
    cat[0].origins += [origin]
    cat[0].preferred_origin_id = origin.resource_id.id
    logger.info('Total number of picks: %d' %
                len(cat[0].preferred_origin().arrivals))

    logger.info('done creating new event or appending to existing event '
                'in %0.3f seconds' % (t11 - t10))

    return cat, stream


__module_name__ = 'picker'

app = Application(module_name=__module_name__)
app.init_module()

# reading application data
settings = app.settings
params = app.settings.picker
logger = app.get_logger(settings.create_event.log_topic,
                        settings.create_event.log_file_name)

app.logger.info('awaiting message from Kafka')
try:
    for msg_in in app.consumer:
        # try:
        cat, st = app.receive_message(msg_in, picker, params=params, app=app)
        # except Exception as e:
        #     logger.error(e)


        app.send_message(cat, st)
        app.logger.info('awaiting message from Kafka')

        logger.info('awaiting Kafka messsages')

except KeyboardInterrupt:
    logger.info('received keyboard interrupt')

finally:
    logger.info('closing Kafka connection')
    app.consumer.close()
    logger.info('connection to Kafka closed')
