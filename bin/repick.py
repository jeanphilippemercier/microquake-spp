#!/usr/bin/env python3

from spp.utils.application import Application
from microquake.waveform.pick import snr_picker
import numpy as np
from microquake.core.event import (Origin, CreationInfo)
from microquake.core import UTCDateTime

import copy

def picker(stream, event, extra_msgs=None, logger=None, params=None, app=None):

    from time import time

    #picks = app.synthetic_arrival_times(o_loc, ot_utc)
    #picks = event.picks
    picks = [pick for pick in event.picks]

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


    return snr_picks_filtered


