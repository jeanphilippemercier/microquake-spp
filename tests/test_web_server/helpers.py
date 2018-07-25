from spp.travel_time import core
from spp.utils import get_stations
#from microquake.core import read, UTCDateTime
#from microquake.core.stream import Stream
from microquake.waveform.pick import SNR_picker, calculate_snr, kurtosis_picker
from microquake.core.event import Pick, make_pick, Arrival
import numpy as np
import os

from obspy.core.utcdatetime import UTCDateTime

from microquake.core.stream import Trace, Stream
#from obspy.core.stream import Stream
#from microquake.waveform.pick import SNR_picker, calculate_snr
import matplotlib.pyplot as plt
import copy
#from lib.waveform import WaveformPlotting as wp

def check_for_dead_trace(tr):
    eps = 1e-6
    data = tr.data.copy()
    mean = np.mean(data)
    max = np.max(data) - mean
    min = np.min(data) - mean
    #print('%s: mean:%f max:%f min:%f' % (tr.get_id(), mean, max, min))
    if max < eps and np.abs(min) < eps:
        #print('** Dead channel')
        return 1
    else:
        return 0

def picks_to_arrivals(picks):
    arrivals = []
    for pick in picks:
        arrival = Arrival()
        arrival.phase = pick.phase_hint
        arrival.pick_id = pick.resource_id.id
        arrivals.append(arrival)
    return arrivals

def copy_picks_to_dict(picks):
    pick_dict = {}
    for pick in picks:
        station = pick.waveform_id.station_code
        phase   = pick.phase_hint
        if station not in pick_dict:
            pick_dict[station]={}
        pick_dict[station][phase]=copy.deepcopy(pick)
    return pick_dict

def plot_profile_with_picks(st, picks=None, origin=None, title=None):

    fname = 'plot_profile_with_picks'

    if origin is None:
        print('%s: need to specify origin' % fname)
        return 0

    if picks is None:
        print(origin.time, origin.loc)
        #picks = get_predicted_picks(st, origin)
        picks = get_predicted_picks(st.composite(), origin)

    plt.clf()

    earliest_pick = UTCDateTime(2070, 1, 1)

    for pick in picks:
        if pick.time < earliest_pick:
            earliest_pick = pick.time

    sorted_p_picks = sorted([pick for pick in picks if pick.phase_hint == 'P'], key=lambda x: x.time)

    pick_dict = copy_picks_to_dict(picks)
    for tr in st.composite():
        pk_p = None
        pk_s = None
        sta = tr.stats.station
        if sta in pick_dict:
            if 'P' in pick_dict[sta]:
                pk_p = pick_dict[sta]['P'].time
            if 'S' in pick_dict[sta]:
                pk_s = pick_dict[sta]['S'].time

        if pk_p is None:
            print('sta: %s pk_p is None' % (tr.get_id()))
            continue

        starttime = tr.stats.starttime

        rt = np.arange(0, tr.stats.npts) / tr.stats.sampling_rate
        t = np.array([starttime + dt for dt in rt])

        d = (pk_p - origin.time) * 5000

        data = tr.data
        data /= np.max(np.abs(data))

        plt.plot(t, tr.data*20 + d, 'k')

        plt.vlines(pk_p, d-20, d+20, colors='r', linestyles=':')
        plt.vlines(pk_s, d-20, d+20, colors='b', linestyles='--')

    stations = st.unique_stations()
    for i,pick in enumerate(sorted_p_picks):
        pk_p = pick.time
        d = (pk_p - origin.time) * 5000
        station = pick.waveform_id.station_code
        if station not in stations:
            continue
        if i%2:
            plt.text(earliest_pick.timestamp + .5, d, station, horizontalalignment='left', \
                     verticalalignment='center',color='red')
        else:
            plt.text(earliest_pick.timestamp - .1, d, station, horizontalalignment='right', \
                     verticalalignment='center',color='red')
    if title:
        plt.title(title)
    plt.xlim(earliest_pick.timestamp - .1, earliest_pick.timestamp + .5)
    plt.show()

def get_predicted_picks(stream, origin):

    predicted_picks = []
    for tr in stream:
        station = tr.stats.station
        ptime = core.get_travel_time_grid(station, origin.loc, phase='P', use_eikonal=False)
        stime = core.get_travel_time_grid(station, origin.loc, phase='S', use_eikonal=False)
        predicted_picks.append( make_pick(origin.time + ptime, phase='P', wave_data=tr) )
        predicted_picks.append( make_pick(origin.time + stime, phase='S', wave_data=tr) )

        #def make_pick(time, phase='P', wave_data=None, SNR=None, mode='automatic', status='preliminary'):

    return predicted_picks


def plot_channels_with_picks(stream, station, picks, title=None):

    fname = 'plot_channels_with_picks'

    extras = {}
    st = stream.select(station=station)

    st2 = st.composite().select(station=station)
    st3 = st + st2

    for tr in st:
        check_for_dead_trace(tr)

    if len(st3) == 0:
        print('%s: sta:%s st3 is empty --> nothing to plot!' % (fname, station))
        return

    if type(picks) is list:
        picks = copy_picks_to_dict(picks)

    pFound = sFound = 0
    if station in picks:
        if 'P' in picks[station]:
            extras['ptime'] = picks[station]['P'].time
            pFound = 1
        if 'S' in picks[station]:
            extras['stime'] = picks[station]['S'].time
            sFound = 1
        if pFound:
            starttime = extras['ptime'] - .02
            starttime = extras['ptime'] - .03
            starttime = extras['ptime'] - .15
            starttime = extras['ptime'] - .5
            endtime   = extras['ptime'] + .20
            endtime   = extras['ptime'] + .30
            endtime   = extras['ptime'] + .50
        elif sFound:
            starttime = extras['stime'] - .2
            endtime   = extras['stime'] + .2
        #st.trim(starttime, endtime, pad=True, fill_value=0.0)
        st3.trim(starttime, endtime, pad=True, fill_value=0.0)

    waveform = wp(stream=st3, color='k', xlabel='Seconds',number_of_ticks=8, tick_rotation=0, title=title, addOverlay=False, extras=extras, outfile=None)
    waveform.plot_waveform()

def check_trace_channels_with_picks(stream, picks):

    fname = 'check_trace_channels_with_picks'

    for station in stream.unique_stations():

        p_time = picks[station]['P'].time
        s_time = picks[station]['S'].time

        win1_start = p_time - .15
        win1_end   = p_time - .05
        win2_start = p_time - .05
        win2_end   = p_time + .05

        st = stream.select(station=station)

        st1 = st.copy()
        st1.trim(win1_start, win1_end, pad=True, fill_value=0.0)
        st2 = st.copy()
        st2.trim(win2_start, win2_end, pad=True, fill_value=0.0)

        extras = {}
        extras['ptime'] = p_time
        extras['stime'] = s_time
        '''
        waveform = wp(stream=st, color='k', xlabel='Seconds',number_of_ticks=8, tick_rotation=0, title='Something else!', addOverlay=False, extras=extras, outfile=None)
        waveform.plot_waveform()
        '''

        for tr in st:
            ch = tr.stats.channel
            d1 = st1.select(channel=ch)[0].data
            d2 = st2.select(channel=ch)[0].data
            m1 = np.mean(d1)
            m2 = np.mean(d2)
            med1 = np.median(d1)
            med2 = np.median(d2)
            max1 = np.fabs(d1.max())
            max2 = np.fabs(d2.max())

            snr = np.fabs(max2/max1)
            #print('sta:%s ch:%s m1:%12.8g med1:%12.8g max1:%12.8g m2:%12.8g med2:%12.8g max2:%12.8g (%12.8g, %12.8g) [%12.8g]' % \
                #(station, ch,m1,med1,max1,m2,med2,max2, max1/med1, max2/med2, snr))

            if snr < 4 :
                print('** Remove channel=[%s] snr=%f' % (tr.get_id(), snr))
                stream.remove(tr)


def calculate_residual(st, picks, origin):

    tot_resid = 0.
    rms_resid = 0.
    npicks = 0
    obs_picks = copy_picks_to_dict(picks)
    syn_picks = copy_picks_to_dict(get_predicted_picks(st, origin))

    for station in obs_picks:
        for phase in obs_picks[station]:
            resid = obs_picks[station][phase].time.timestamp - syn_picks[station][phase].time.timestamp
            tot_resid += resid
            rms_resid += resid*resid
            npicks += 1
            print('%2s: [%s] ims:%s  new:%s ims-new:%8.4f' % (station, phase, \
                obs_picks[station][phase].time, syn_picks[station][phase].time, resid))

    rms_resid /= float(npicks)
    rms_resid =  np.sqrt(rms_resid)
    print('== tot_resid=%8.4f rms_resid=%8.4f' % (tot_resid, rms_resid))

def clean_picks(st, picks, preWl=.03, postWl=.03, thresh=3):
    fname = 'clean_picks'

    pick_dict = copy_picks_to_dict(picks)

    for station in pick_dict:
        trs = st.select(station=station)
        if len(trs) == 0:
            print('%s: sta:%s has picks but is not in stream!' % (fname, station))
            continue
        if 'P' in pick_dict[station]:
            p_snr  = calculate_snr(trs, pick_dict[station]['P'].time, preWl, postWl)
            if p_snr < thresh:
                print('sta:%s ** P snr [%.1f] is < thresh!' % (station,p_snr))
                del(pick_dict[station]['P'])
            print('sta:%s [P] snr [%.1f]' % (station,p_snr))
        if 'S' in pick_dict[station]:
            s_snr  = calculate_snr(trs, pick_dict[station]['S'].time, preWl, postWl)
            if s_snr < thresh:
                print('sta:%s ** S snr  [%.1f]is < thresh!' % (station,s_snr))
                del(pick_dict[station]['S'])
            print('sta:%s [S] snr [%.1f]' % (station,s_snr))
            #plot_channels_with_picks(trs, station, snr_picks, title=title)

    cleaned_picks = []
    for station in pick_dict:
        for phase in pick_dict[station]:
            cleaned_picks.append(pick_dict[station][phase])

    return cleaned_picks

    '''
    station_distance = {}
    for station in get_stations().stations():
        dx = station.loc[0] - location[0]
        dy = station.loc[1] - location[1]
        dz = station.loc[2] - location[2]
        dist = np.sqrt(dx*dx + dy*dy + dz*dz)
        station_distance[station.code] = dist
        #print(station.code, station_distance)
    '''
