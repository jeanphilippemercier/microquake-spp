from spp.travel_time import core
from spp.utils import get_stations
from importlib import reload
from datetime import datetime
from microquake.core import read
import numpy as np
import os

from microquake.waveform.pick import SNR_picker
import matplotlib.pyplot as plt
from microquake.realtime.signal import kurtosis
from microquake.core import read_events

from microquake import nlloc
from microquake.core import ctl

from microquake.core.event import Arrival, Origin, Event

#from helpers import *

reload(core)

data_dir   = os.environ['SPP_DATA']
config_dir = os.environ['SPP_CONFIG']

if __name__ == "__main__":

    location = "651280, 4767430, -140"
    # origin_time = datetime(2018, 4, 14, 19, 44, 22, 92372)
    stations = get_stations()

    data_dir = os.environ['SPP_DATA']
    config_dir = os.environ['SPP_CONFIG']

    st = read(os.path.join(data_dir, '2018-04-15_034422.mseed'),
               format='MSEED')

    buf = BytesIO()
    st.write(buf, format='MSEED')
    data = buf.getvalue()

    from spp.scripts import picker

    # testing the script receiving data in binary format
    cat = picker.predict_travel_time(location, data)




    # cat = core.create_event(st2, location)

    event_file = '../../data/2018-04-15_034422.xml'
    event = read_events(event_file, format='QUAKEML')[0]
    ims_picks = copy_picks_to_dict(event.picks)

    st = read(os.path.join(data_dir, '2018-04-15_034422.mseed'), format='MSEED')

    stn_list = ims_picks.keys()
    tr_stn_list = set()
    for tr in st:
        tr_stn_list.add(tr.stats.station)

    missing_stns = []
    for station in stn_list:
        if station not in tr_stn_list:
            print('stn:%2s is missing from mseed!' % station)
            missing_stns.append(station)

    tmp = []
    for pick in event.picks:
        if pick.phase_hint == 'P':
            tmp.append(pick)

    from collections import OrderedDict
    master_stations = OrderedDict()
    for pick in sorted(tmp, key=lambda x: x.time.timestamp):
        station = pick.waveform_id.station_code
        if station not in missing_stns:
            master_stations[station] = {}
            master_stations[station]['P'] = ims_picks[station]['P']
            master_stations[station]['S'] = ims_picks[station]['S']
            
    #for station in master_stations:
        #print('sta:%s P:%s S:%s' % (station, master_stations[station]['P'].time, master_stations[station]['S'].time))

    location = [651280, 4767430, -140]
    #core.init_travel_time()

    # 1. Stack traces to get origin time + predicted picks for origin time + loc:
    cat = core.create_event(st, location)
    origin_time = cat[0].origins[0].time
    picks = cat[0].picks
    # prelim_picks = predicted for specified location + calculated origin_time
    prelim_picks = copy_picks_to_dict(cat[0].picks)

    plot_profile_with_picks(st, picks=event.picks, origin=cat[0].origins[0], title='2018-04-14 03:44:22 [IMS picks]')
    #st.filter("bandpass", freqmin=40, freqmax=350)
    #plot_profile_with_picks(st, picks=event.picks, origin=cat[0].origins[0], title='2018-04-14 03:44:22 [IMS picks]')
    #exit()
    #plot_profile_with_picks(st, picks=None, origin=cat[0].origins[0], title='2018-04-14 03:44:22 [Pred picks]')

    # 2. Randomly perturb the IMS picks:
    import random
    perturbed_picks = []
    for pick in event.picks:
        #x = random.random()
        x = random.randint(1,100)
        size = .02
        if x < 50 :
            t_perturb = size * random.random()
        else:
            t_perturb = -size * random.random()
        pick.time += t_perturb
        perturbed_picks.append(pick)
        
    plot_profile_with_picks(st, picks=perturbed_picks, origin=cat[0].origins[0], title='2018-04-14 03:44:22 [IMS perturbed picks]')

    # 3. Repick using SNR picker:

    cat[0].picks = perturbed_picks
    # Separately tune picker for P
    #st2 = st.select(station='23')
    #st2 = st.select(station='40')
    st2 = st
    (cat_picked, snrs) = SNR_picker(st2, cat, SNR_dt=np.linspace(-.05, .05, 200), SNR_window=(1e-3, 5e-3))
    p_picks = []
    for pick in cat_picked[0].picks:
        if pick.phase_hint == 'P':
            p_picks.append(pick)

    # Separately tune picker for S
    #(cat_picked, snrs) = SNR_picker(st2, cat, SNR_dt=np.linspace(-.1, .1, 200), SNR_window=(1e-3, 10e-3))
    (cat_picked, snrs) = SNR_picker(st2, cat, SNR_dt=np.linspace(-.05, .05, 200), SNR_window=(1e-3, 10e-3))
    #(cat_picked, snrs) = SNR_picker(st2, cat, SNR_dt=np.linspace(-.05, .05, 200), SNR_window=(1e-3, 20e-3))
    s_picks = []
    for pick in cat_picked[0].picks:
        if pick.phase_hint == 'S':
            s_picks.append(pick)

    cat_picked[0].picks = p_picks + s_picks

    snr_picks = copy_picks_to_dict(p_picks + s_picks)
    plot_profile_with_picks(st, picks=cat_picked[0].picks, origin=cat[0].origins[0], title='2018-04-14 03:44:22 [SNR picks]')
    # Output resids between IMS and SNR picks:

    for pick in sorted(p_picks, key=lambda x: x.time.timestamp):
        station = pick.waveform_id.station_code
        if station in missing_stns:
            print('sta: %s is missing' % station)
        else:
            p_pick = snr_picks[station]['P'].time.datetime.time()
            p_snr  = float(snr_picks[station]['P'].comments[0].text.split('SNR=')[1])
            s_pick = snr_picks[station]['S'].time.datetime.time()
            s_snr  = float(snr_picks[station]['S'].comments[0].text.split('SNR=')[1])
            ims_p  = ims_picks[station]['P'].time.datetime.time()
            ims_s  = ims_picks[station]['S'].time.datetime.time()
            res_p  = ims_picks[station]['P'].time.timestamp - snr_picks[station]['P'].time.timestamp
            res_s  = ims_picks[station]['S'].time.timestamp - snr_picks[station]['S'].time.timestamp

            print('%2s: [P] ims:%s snr:%s res:%7.4f [S] ims:%s snr:%s res:%7.4f' % 
                 (station, ims_p, p_pick, res_p, ims_s, s_pick, res_s))
            '''
            print('%2s: [P] ims:%s snr:%s [SNR: %.1f] res:%7.4f' % 
                 (station, ims_p, p_pick, p_snr, res_p))
            '''
    ##########
    # INSERT PICKER HERE, the picker function should take two non keyword
    # arguments (a stream and a catalogue and as many as keyword arguments as
    # you want. the keyword arguments will be passed from a config file
    # For instance in let say we get the parameter from a yaml file we would get
    # >> import yaml
    # >> params = yaml.load(...)
    # >> cat_out = picker(stream, catalogue, **params[picker][kwargs]
    # the SNR picker currently returns a catalogue but could probably simply
    # return a list of picks.
    # example of how I use the SNR picker below.
    # Note 1: The SNR picker is not optimized and does not currently work well.
    # Note 2: you will probably want to filter the waveforms. I have tried
    # filtering between 50 and 300 Hz. You may want to play more with the filter
    # Note, that the origin time should also be corrected following the
    # picking. Bias which will be the mean of the difference between the
    # initial picks and the new picks (Bias = new picks - initial picks) should
    #  be added to the origin time.

    '''

    (cat_picked, snr) = SNR_picker(st2.filter('bandpass', freqmin=50,
                                              freqmax=300), cat,
                                   SNR_dt=np.linspace(-10e-3, 10e-3, 100),
                                   SNR_window=(-5e-3, 20e-3))
    '''



    #########

    config_file = config_dir + '/input.xml'
    params = ctl.parse_control_file(config_file)
    nll_opts = nlloc.init_nlloc_from_params(params)

    # The following line only needs to be run once. It creates the base directory
    
    #nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)

# Need to pass in an event with a preferred origin

    print(cat[0].origins[0])
    #event_new = copy.deepcopy(event)

    event_new = Event()
    origin = cat[0].origins[0]

    event_new.origins.append(origin)
    for origin in event_new.origins:
        print('Heres an origin:')
        print(origin)
    print('Done')

    event_new.picks = cat_picked[0].picks
    from microquake.core.event import ResourceIdentifier
    #event_new.preferred_origin_id = ResourceIdentifier(referred_object=origin)
    event_new.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id)

    arrivals = []
    for pick in event_new.picks:
        arrival = Arrival()
        arrival.phase = pick.phase_hint
        arrival.pick_id = pick.resource_id.id
        arrivals.append(arrival)
        #print(pick)

    event_new.preferred_origin().arrivals = arrivals
    print('Event:')
    print(event_new)
    print('Preferred Origin:')
    print(event_new.preferred_origin())

    #origin.update_arrivals(site)
    ot_event = nll_opts.run_event(event_new)[0]

    print('**** Here are the origins after nlloc')
    for origin in ot_event.origins:
        print(origin)

    print('Preferred Origin:')
    print(ot_event.preferred_origin())

# Does nlloc return new picks ??
    plot_profile_with_picks(st, picks=None, origin=ot_event.preferred_origin(), title='2018-04-14 03:44:22 [NLLOC origin]')
    exit()


    nlloc_picks = []
    for pick in cat_picked[0].picks:
        station = pick.waveform_id.station_code
        #tt= core.get_travel_time_grid(station, origin.loc, phase=pick.phase_hint, use_eikonal=True)
        tt= core.get_travel_time_grid(station, origin.loc, phase=pick.phase_hint, use_eikonal=False)
        pick.time = origin.time + tt
        print(origin.time, tt, pick.time)
        nlloc_picks.append(copy.deepcopy(pick))

    plt.clf()

    for tr in st.composite():
        for pk in cat_picked[0].picks:
            if pk.waveform_id.station_code == tr.stats.station:
                if pk.phase_hint == "P":
                    pk_p = pk.time
                else:
                    pk_s = pk.time

        starttime = tr.stats.starttime
        rt = np.arange(0, tr.stats.npts) / tr.stats.sampling_rate
        t = np.array([starttime + dt for dt in rt])

        d = (pk_p - origin_time) * 5000

        data = tr.data
        data /= np.max(np.abs(data))

        plt.plot(t, tr.data*20 + d, 'k')
        plt.vlines(pk_p, d-20, d+20, colors='r', linestyles=':')
        plt.vlines(pk_s, d-20, d+20, colors='r', linestyles='--')


    plt.show()
    exit()
