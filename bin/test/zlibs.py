#!/usr/bin/env python3

from spp.utils.application import Application
from microquake.waveform.pick import snr_picker
import numpy as np
from microquake.core.event import (Origin, CreationInfo)
from microquake.core import UTCDateTime

from helpers import *

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

    #print("Here are the Predicted picks")
    #for pk in picks:
        #print("sta:%s pha:%s time:%s" % (pk.waveform_id.station_code, pk.phase_hint, pk.time))

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

    snrs_p, p_snr_picks = snr_picker(st, picks,
                                     snr_dt=search_window,
                                     snr_window=snr_window,  filter='P')

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
                                   snr_window=snr_window, filter='S')

    t7 = time()
    logger.info('done picking S-wave in %0.3f seconds' % (t7 - t6))

    snr_picks = p_snr_picks + s_snr_picks
    snrs = snrs_p + snrs_s

    snr_picks_filtered = [snr_pick for (snr_pick, snr)
                          in zip(snr_picks, snrs)
                          if snr > params.snr_threshold]

    logger.info('creating arrivals')
    t8 = time()
    arrivals = app.create_arrivals_from_picks(snr_picks_filtered, loc,
                                              ot_utc)
    t9 = time()
    logger.info('done creating arrivals in %0.3f seconds' % (t9 - t8))

    logger.info('creating new event or appending to existing event')
    t10 = time()

    t11 = time()

    logger.info('Origin time: %s' % ot_utc)
    logger.info('Total number of picks: %d' %
                len(cat[0].preferred_origin().arrivals))

    logger.info('done creating new event or appending to existing event '
                'in %0.3f seconds' % (t11 - t10))


    # TODO: Need to check the event database for IMS event.
    # if an event exist, 1) get the event, 2) create a new origin with
    # information from interloc, 3) append picks to event, and 4) append
    # arrivals to the new origin


    origin = Origin()
    origin.time = ot_utc
    origin.x = o_loc[0]
    origin.y = o_loc[1]
    origin.z = o_loc[2]

    print(arrivals[0].__dict__)

    origin.arrivals = arrivals
    origin.evaluation_mode = 'automatic'
    origin.evaluation_status = 'preliminary'
    origin.creation_info = CreationInfo(creation_time=UTCDateTime.now())

    cat[0].picks += snr_picks_filtered
    cat[0].origins += [origin]
    #cat[0].origins.append(origin)

    cat[0].preferred_origin_id = origin.resource_id.id

    #return snr_picks_filtered

    return cat, stream



from microquake.nlloc import NLL, calculate_uncertainty

def location(cat=None, stream=None, extra_msgs=None, logger=None, nll=None,
             params=None, project_code=None):

    from time import time
    from IPython.core.debugger import Tracer

    #logger.info('unpacking the data received from Kafka topic <%s>'
                #% settings.nlloc.kafka_consumer_topic)

    logger.info('running NonLinLoc')
    t0 = time()
    cat_out = nll.run_event(cat[0].copy())
    t1 = time()
    logger.info('done running NonLinLoc in %0.3f seconds' % (t1 - t0))

    base_folder = params.nll_base

    logger.info('calculating Uncertainty')
    t2 = time()
    origin_uncertainty = calculate_uncertainty(cat_out[0], base_folder,
                                               project_code,
                                               perturbation=5,
                                               pick_uncertainty=1e-3)

    cat_out[0].preferred_origin().origin_uncertainty = origin_uncertainty
    t3 = time()
    logger.info('done calculating uncertainty in %0.3f seconds' % (t3 - t2))

    return cat_out, stream

