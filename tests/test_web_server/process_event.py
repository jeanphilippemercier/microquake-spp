from spp.travel_time import core
from spp.utils import get_stations
from datetime import datetime
from microquake.core import ctl
from microquake.core import read, UTCDateTime
from microquake.core.stream import Stream
from microquake.core.event import Arrival, Event, Origin
from microquake.core.event import ResourceIdentifier
from obspy.core.event import Event as obsEvent
from microquake.waveform.pick import SNR_picker, calculate_snr, kurtosis_picker
import numpy as np
import os

from importlib import reload
reload(core)

from web_client import get_stream_from_mongo

#from IPython.core.debugger import Tracer
#from microquake.realtime.signal import kurtosis
# Don't use!
#from microquake.core import read_events
from obspy.core.event import read_events
# MTH: the diff is if you use obspy read_events, then you need to drill down to get
#      to the extra[] dict values, e.g.:
#      pick.extra.method.value vs pick.method (since we've implemented useful __getattr__ on microquake Pick)

from microquake import nlloc

#from obspy.core.event import Catalog

from helpers import *

import logging
logger = logging.getLogger()
#logger.setLevel(logging.WARNING)
#print(logger.level)
import matplotlib.pyplot as plt

data_dir   = os.environ['SPP_DATA']
config_dir = os.environ['SPP_CONFIG']

def main():
    # Big event
    x = 651298
    y = 4767394
    z = -148
    timestamp = 1527072662.2131672
    #origin.time = UTCDateTime( datetime(2018, 5, 23, 10, 51, 2, 213167) )
    make_event( np.array([x,y,z,timestamp]) )
    exit()

    # Small event
    #time = UTCDateTime( datetime(2018, 5, 23, 10, 51, 3, 765333) )
    x = 651280
    y = 4767400
    z = -200
    timestamp = 1527072663.765333
    make_event( np.array([x,y,z,timestamp]) )

def make_event(xyzt_array):
    plot_profiles = 0

    fname = 'make_event'
    if xyzt_array.size != 4:
        logger.error('%s: expecting 4 inputs = {x, y, z, t}' % fname)
        exit(2)
    origin = Origin()
    origin.time = UTCDateTime( xyzt_array[3])
    origin.x = xyzt_array[0]
    origin.y = xyzt_array[1]
    origin.z = xyzt_array[2]
    #event = Event()
    event = obsEvent()
    event.origins = [origin]
    # Don't use the method below - it bungles the pref id
    #event.preferred_origin_id = ResourceIdentifier(referred_object=origin)
    # Either of these methods seems to work:
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id)
    #event.preferred_origin_id = origin.resource_id
    print(event)
    print()
    print()
    print(origin)
    origin.method = 'InterLoc Event'
    print('origins[0] id:%s' % event.origins[0].resource_id.id)
    print('pref_orig  id:%s' % event.preferred_origin().resource_id.id)
    print('pref_orig  id:%s' % event.preferred_origin().resource_id)
    print('pref_orig  id:%s' % event.preferred_origin_id)
    event.write('event.xml', format='quakeml')
    event_read = read_events('event.xml', format='QUAKEML')[0]
    print('read pref  id:%s' % event_read.preferred_origin_id)
    print('pref_orig  id:%s' % event.preferred_origin_id)
    print('type:%s' % type(event.preferred_origin_id))

    starttime = origin.time - 0.1
    endtime   = origin.time + 0.9
    st1 = get_stream_from_mongo(starttime, endtime)

    #starttime = origin.time - 10
    #endtime   = origin.time + 10
    #st = get_stream_from_mongo(starttime, endtime, chan=chan)

    for tr in st1:
        print('id:%s \t %s - %s' % (tr.get_id(), tr.stats.starttime, tr.stats.endtime))
        #tr.plot()
    #exit()

    st1.filter("bandpass", freqmin=80.0, freqmax=600.0, corners=4)
    # ATODO MTH: get_stream() is returning obspy Stream, not microquake
    st = Stream(st1.copy())

    title = 'InterLoc Orig <%.0f, %.0f, %.0f> %s' % (origin.x, origin.y, origin.z, origin.time)
    print(type(st1))
    print(type(st))

    st.write("event.mseed")
    if plot_profiles:
        plot_profile_with_picks(st, picks=None, origin=origin, title=title)

  # 1. Stack traces to get origin time + prelim predicted picks for origin time + loc:
    event2 = core.create_event(st, origin.loc)[0]
    origin2 = event2.origins[0]

    event.origins.append(origin2)
    event.picks = event2.picks
    event.write('event2.xml', format='quakeml')

    # prelim_picks = predicted for specified location + calculated origin_time
    prelim_picks = event2.picks

    check_trace_channels_with_picks(st, copy_picks_to_dict(prelim_picks))
    title = 'create_event %s' % (origin2.time)
    if plot_profiles:
        plot_profile_with_picks(st, picks=prelim_picks, origin=origin, title=title + ' prelim picks, clean ch')
    #exit()

    picks = copy_picks_to_dict(prelim_picks)

  # 2. Repick:

    #st2 = st
    old_picks = [picks[station]['P'] for station in picks]
    p_picks = SNR_picker(st, old_picks, SNR_dt=np.linspace(-.05, .05, 200), SNR_window=(10e-3, 5e-3))
    #p_picks = kurtosis_picker(st2, old_picks)

    # Separately tune picker for S
    #(cat_picked, snrs) = SNR_picker(st2, cat, SNR_dt=np.linspace(-.1, .1, 200), SNR_window=(1e-3, 10e-3))

    old_picks = [picks[station]['S'] for station in picks]
    s_picks = SNR_picker(st, old_picks, SNR_dt=np.linspace(-.05, .05, 200), SNR_window=(20e-3, 10e-3))
    #s_picks = kurtosis_picker(st2, old_picks)

    new_picks = p_picks + s_picks

    if plot_profiles:
        plot_profile_with_picks(st, picks=new_picks, origin=origin, title='[SNR picks] f:80-600Hz')
    #exit()

  # 4. Clean up picks:
    #cleaned_picks = clean_picks(st2, new_picks, preWl=.03, postWl=.03, thresh=6.5)
    cleaned_picks = clean_picks(st, new_picks, preWl=.06, postWl=.12, thresh=3)
    #cleaned_picks = clean_picks(st2, new_picks, preWl=.03, postWl=.03, thresh=3)
    foo_picks = copy_picks_to_dict(new_picks)
    #for station in ['24', '32','72']:
    #for station in ['24']:
        #title = 'sta:%s' % station
        #print(foo_picks[station]['S'].pick)
        #plot_channels_with_picks(st, station, picks=new_picks, title=title)

    plot_profile_with_picks(st, picks=cleaned_picks, origin=origin, title='[Cleaned SNR picks] f:80-600Hz')

    event.picks += copy.deepcopy(cleaned_picks)
    event.origins[1].arrivals = picks_to_arrivals(cleaned_picks) 
    event.preferred_origin_id = event.origins[1].resource_id
    event.write('event3.xml', format='quakeml')

    #########

  # 5. Relocate event with cleaned picks

    config_file = config_dir + '/input.xml'
    config_file = config_dir + '/project.xml'

    params = ctl.parse_control_file(config_file)
    nll_opts = nlloc.init_nlloc_from_params(params)

    # The following line only needs to be run once. It creates the base directory
    # MTH: this will re-create the station time grids in spp common/NLL/time:
    #nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)
    #exit()

# Need to pass in an event with a preferred origin + arrivals

    #event_new = cat[0].copy()
    #event_new.preferred_origin_id = event_new.origins[0].resource_id.id

    #event_new.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id)
    #origin.update_arrivals(site)
    #ot_event = nll_opts.run_event(event_new)[0]
    # MTH: run_event will run NLLOC in spp common/NLL/tmp.../loc and will scan the hypo file there
    #cat_new = nll_opts.run_event(event_new)[0]
    #cat_new = nll_opts.run_event(event_new)[0]
    #cat_new.origins[1].method = 'NLLOC run_event'

    event_new = nll_opts.run_event(event)[0]
    print('MTH: NLLOC is Done, now write out event4.xml')
    event_new.origins[2].method = 'NLLOC run_event'
    event_new.write('event4.xml', format='quakeml')

    print( event_new.preferred_origin().resource_id.id)
    event_read = read_events('event4.xml', format='QUAKEML')[0]
    print(event_read.preferred_origin().resource_id.id)
    exit()

    print('cat_new : %s' % cat_new.preferred_origin().resource_id.id)
    print('cat_read: %s' % cat_read.preferred_origin().resource_id.id)
    print('type(cat_new)=%s' % type(cat_new))
    print('type(cat_read)=%s' % type(cat_read))
    cat_read.write('foo1.xml', format='quakeml')
    #exit()

    import pickle as pickle
    #qbytes = pickle.dumps(cat_read)
    qbytes = pickle.dumps(cat_new)
    cat_2 = pickle.loads(qbytes)
    print('   cat_2: %s' % cat_2.preferred_origin().resource_id.id)
    cat_2.write('foo2.xml', format='quakeml')

    exit()

    for origin in cat_read.origins:
        if 'NLLOC' in origin.method:
            print('NLLOC origin:')
            print(origin)
    #newPicks = Pick.filter_picks(cat_read[0].picks, 'SNR')
    #newPicks = Pick.filter_picks(cat_read[0].picks, 'theoretical')

    #for pick in newPicks:
        #print(pick)

    print(' cat_new.preferred_origin.id=[%s]' % cat_new.preferred_origin().resource_id.id)
    print('cat_read.preferred_origin.id=[%s]' % cat_read.preferred_origin().resource_id.id)

    print('cat_2.preferred_origin.id=[%s]' % cat_2.preferred_origin().resource_id.id)

    '''
   with open(filename, 'wb') as of:
        pickle.dump(grid, of, protocol=protocol)
    '''

    exit()


  # 6. Compare resid between cleaned picks and predicted picks for nlloc origin:
    resid = calculate_residual(st, cleaned_picks, ot_event.preferred_origin()) 


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


    exit()

if __name__ == "__main__":
    main()

