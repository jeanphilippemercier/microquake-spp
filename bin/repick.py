#!/usr/bin/env python3

from spp.utils.application import Application
from microquake.waveform.pick import snr_picker
import numpy as np
from microquake.core.event import (Origin, CreationInfo)
from microquake.core import UTCDateTime

def picker(stream, event, extra_msgs=None, logger=None, params=None, app=None):

    from time import time

    #picks = app.synthetic_arrival_times(o_loc, ot_utc)
    picks = event.picks

    freq_min = params.waveform_filter.frequency_min
    freq_max = params.waveform_filter.frequency_max

    st = stream.copy().detrend('demean')
    st = st.taper(max_percentage=0.1, max_length=0.01)
    #st = st.filter('bandpass', freqmin=freq_min, freqmax=freq_max)


    search_window = np.arange(params.p_wave.search_window.start,
                              params.p_wave.search_window.end,
                              params.p_wave.search_window.resolution)

    snr_window = (params.p_wave.snr_window.noise,
                  params.p_wave.snr_window.signal)


    snrs_p, p_snr_picks = snr_picker(st, picks,
                                     snr_dt=search_window,
                                     snr_window=snr_window,  filter='P')

    search_window = np.arange(params.s_wave.search_window.start,
                              params.s_wave.search_window.end,
                              params.s_wave.search_window.resolution)

    snr_window = (params.s_wave.snr_window.noise,
                  params.s_wave.snr_window.signal)

    snrs_s, s_snr_picks = snr_picker(st, picks,
                                   snr_dt=search_window,
                                   snr_window=snr_window, filter='S')
    snr_picks = p_snr_picks + s_snr_picks
    snrs = snrs_p + snrs_s

    snr_picks_filtered = [snr_pick for (snr_pick, snr)
                          in zip(snr_picks, snrs)
                          if snr > params.snr_threshold]

    #from helpers import plot_profile_with_picks, plot_channels_with_picks

    #print("snr_thresh:%f" % params.snr_threshold)
    #snr_picks_removed = [snr_pick for (snr_pick, snr) in zip(snr_picks, snrs)
                          #if snr <= params.snr_threshold]
    #for pick in snr_picks_removed:
        #sta = pick.waveform_id.station_code
        #print("Remove pick: sta:%s pha:%s time:%s" % (pick.waveform_id.station_code, pick.phase_hint,\
        #                                                      pick.time))
        #plot_channels_with_picks(st, sta, snr_picks, title="sta:%s Bad pick" % (sta))

    #for pick in snr_picks_filtered:
        #sta = pick.waveform_id.station_code
        #print("Pick sta:%s [%s] polarity:%s time:%s" % (sta, pick.phase_hint, pick.polarity, pick.time))
        #print(pick.comments[0])

    #plot_profile_with_picks(st, picks=snr_picks_filtered, origin=event.preferred_origin(), title="SNR picks")

    return snr_picks_filtered


