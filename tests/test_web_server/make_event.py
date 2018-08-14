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

#from web_client import get_stream_from_mongo
from web_client import *

from microquake.core import read_events as micro_read_events
from obspy.core.event import read_events
from microquake import nlloc

from helpers import *

from io import BytesIO

#from liblog import getLogger
#logger = getLogger()
import logging
logger = logging.getLogger()
#print('process_event: inherits log level=[%s]' % (logger.getEffectiveLevel()))
print('import process_event: inherits log level=[%s]' % get_log_level(logger.getEffectiveLevel()))

config_dir = os.environ['SPP_CONFIG']

def make_event(xyzt_array, plot_profiles=False, insert_event=False):
#MTH: You have to have these 2 set to get pretty print output of Origin:  -->
        #<evaluationMode>manual</evaluationMode>
        #<evaluationStatus>reviewed</evaluationStatus>

    fname = 'make_event'

    #print('%s: LOG LEVEL=%s' % (fname, get_log_level(logger.getEffectiveLevel())))

    if xyzt_array.size != 4:
        logger.error('%s: expecting 4 inputs = {x, y, z, t}' % fname)
        #logger.error('%s: expecting 5 inputs = {x, y, z, t, intensity}' % fname)
        exit(2)
    origin = Origin()
    origin.time = UTCDateTime( xyzt_array[3])
    origin.x = xyzt_array[0]
    origin.y = xyzt_array[1]
    origin.z = xyzt_array[2]
    #print(origin)
    event = Event()
    #event = obsEvent()
    event.origins = [origin]
    # Don't use the method below - it bungles the pref id
    #event.preferred_origin_id = ResourceIdentifier(referred_object=origin)
    # Either of these methods seems to work:
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id)
    #event.preferred_origin_id = origin.resource_id
    #print(event)
    origin.method = 'InterLoc Event'

    logger.info('%s: Start from InterLoc Origin:' % fname)
    logger.info(origin)
    event.write('event.xml', format='quakeml')
    logger.info('%s: Write InterLoc Origin to event.xml' % fname)
    #event_read = read_events('event.xml', format='QUAKEML')[0]
    #print('read pref  id:%s' % event_read.preferred_origin_id)

    starttime = origin.time - 0.1
    endtime   = origin.time + 0.9
    logger.info('%s: call get_stream_from_mongo(starttime=[%s] endtime=[%s]' % (fname, starttime, endtime))
    st1 = get_stream_from_mongo(starttime, endtime)
    if st1 is None:
        logger.info('%s: get_stream_from_mongo returned None --> return' % fname)
        return

    for tr in st1:
        logger.info('id:%s \t %s - %s' % (tr.get_id(), tr.stats.starttime, tr.stats.endtime))
        #tr.plot()

    st1.filter("bandpass", freqmin=80.0, freqmax=600.0, corners=4)
    # ATODO MTH: get_stream() is returning obspy Stream, not microquake
    st = Stream(st1.copy())

    title = 'InterLoc Orig <%.0f, %.0f, %.0f> %s' % (origin.x, origin.y, origin.z, origin.time)

    st.write("event.mseed")
    if plot_profiles:
        plot_profile_with_picks(st, picks=None, origin=origin, title=title)

    #exit()

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

 # Make the context mseed
    sorted_p_picks = sorted([pick for pick in cleaned_picks if pick.phase_hint == 'P'], key=lambda x: x.time)
    first_pick = sorted_p_picks[0]
    first_sta  = first_pick.waveform_id.station_code
#MTH: hard-coding for test only.  Issue is that interLoc locn causes sta=89 to be chosen first and
#  it has issues on the server
    #first_sta  = '32'

    starttime = origin.time - 10
    endtime   = origin.time + 10
    st_temp   = get_stream_from_mongo(starttime, endtime, sta=first_sta)
    st_new = Stream(st_temp).composite()
    #st_new.plot()
    #exit()
    #st_new.decimate(factor=10)
    #st_new.plot()
    st_new.write("event_context.mseed")

    #plot_profile_with_picks(st, picks=cleaned_picks, origin=origin, title='[Cleaned SNR picks] f:80-600Hz')

    event.picks += copy.deepcopy(cleaned_picks)
    event.origins[1].arrivals = picks_to_arrivals(cleaned_picks) 
    event.preferred_origin_id = event.origins[1].resource_id
    event.write('event3.xml', format='quakeml')
    #for arr in event.origins[1].arrivals:
        #logger.debug('arr_id=%s --> pick_id=%s' % (arr.resource_id.id, arr.pick_id))
        #pk = arr.pick_id.get_referred_object()
        #logger.debug(pk)
    #exit()

# Try to insert event (and then return event_id) or, if event already exists,
#   just get event_id

    event_id = None
    if insert_event:
        logger.info('%s: Call to putEvent(event3.xml, event.mseed, event_context.mseed)' % fname)
        result = post_data("putEvent", build_event_data('event3.xml', 'event.mseed', 'event_context.mseed'))
        logger.info('%s: Call to putEvent --> returned result code=%d' % (fname, result.code))
        if result.code != 200:
            logger.error('%s: post_data returned result code=%d != 200!' % (fname, result.code))

        read_string  = result.read().decode('utf-8')
        d = json.loads(read_string)
        event_id = d['event_id']
        logger.info('%s: putEvent returned event_id=[%s] with msg:%s' % (fname, event_id, d['message']))

    #exit()

    #########

  # 5. Relocate event with cleaned picks

    config_file = config_dir + '/input.xml'
    config_file = config_dir + '/project.xml'

    logger.info('%s: Call NLLOC with event: pref_origin=%s' % (fname, event.preferred_origin().resource_id.id))
    resource = event.preferred_origin().resource_id
    print(type(resource))
    pref_orig= resource.get_referred_object()
    print(type(pref_orig))
    print(pref_orig)

    params = ctl.parse_control_file(config_file)
    logger.info('%s: Call NLLOC init from params' % fname)
    nll_opts = nlloc.init_nlloc_from_params(params)
    logger.info('%s: NLLOC opts are loaded --> call run_event to relocate' % fname)

    # The following line only needs to be run once. It creates the base directory
    # MTH: this will re-create the station time grids in spp common/NLL/time:
    #nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)
    #exit()

# Need to pass in an event with a preferred origin + arrivals
    event_new = nll_opts.run_event(event)[0]
    logger.info('%s: NLLOC is Done, now write out event4.xml' % fname)
    #exit()

  # Compute magnitude using nlloc origin
    from microquake.waveform.mag import moment_magnitude
    from spp.utils import get_stations
    site = get_stations()
    event_new = moment_magnitude(st, event_new, site, vp=5200, vs=5200/np.sqrt(3))
    event_new.origins[2].method = 'NLLOC run_event'
    event_new.write('event4.xml', format='quakeml')

    logger.info('%s: event4: preferred origin_id=%s' % (fname, event_new.preferred_origin().resource_id.id))
    event_read = read_events('event4.xml', format='QUAKEML')[0]
    logger.info('%s: event_read: preferred origin_id=%s' % (fname, event_read.preferred_origin().resource_id.id))

    #result2 = post_data("updateEvent", build_update_event_data(event_id, 'event4.xml', 'event.mseed', 'event_context.mseed'))
    #print(result2)

    if insert_event:
        if event_id is None:
            logger.error('%s: Unable to update event --> event_id is None!' % (fname))
            exit(1)
        
        logger.info('%s: Try to update event id:%s with event4.xml' % (fname, event_id))
        result = post_data("updateEvent", build_update_event_data(event_id, 'event4.xml'))
        logger.info('%s: Call to updateEvent --> returned result code=%d' % (fname, result.code))
        if result.code != 200:
            logger.error('%s: post_data returned result code=%d != 200!' % (fname, result.code))
            exit(1)

        read_string  = result.read().decode('utf-8')
        d = json.loads(read_string)
        event_id = d['event_id']
        logger.info('%s: update event_id=[%s] returned msg:%s' % (fname, event_id, d['message']))

    exit()

    import pickle as pickle
    #qbytes = pickle.dumps(cat_read)
    qbytes = pickle.dumps(cat_new)
    cat_2 = pickle.loads(qbytes)
    print('   cat_2: %s' % cat_2.preferred_origin().resource_id.id)
    cat_2.write('foo2.xml', format='quakeml')

    exit()


  # 6. Compare resid between cleaned picks and predicted picks for nlloc origin:
    resid = calculate_residual(st, cleaned_picks, ot_event.preferred_origin()) 

# Does nlloc return new picks ??
    plot_profile_with_picks(st, picks=None, origin=ot_event.preferred_origin(), title='2018-04-14 03:44:22 [NLLOC origin]')

if __name__ == "__main__":
    main()

