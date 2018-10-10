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

config_dir = os.environ['SPP_CONFIG']

#from spp.utils import logger as log
#logger = log.get_logger("make_event", 'make_event.log')

from liblog import getLogger
import logging
logger = getLogger()

def make_event(xyzt_array, plot_profiles=False, insert_event=False):
#MTH: You have to have these 2 set to get pretty print output of Origin:  -->
        #<evaluationMode>manual</evaluationMode>
        #<evaluationStatus>reviewed</evaluationStatus>

    fname = 'make_event'

    #print("%s: level:%s" % (fname, get_log_level(logger.getEffectiveLevel())))
    #exit()

    if xyzt_array.size != 4:
        logger.error('%s: expecting 4 inputs = {x, y, z, t}' % fname)
        #logger.error('%s: expecting 5 inputs = {x, y, z, t, intensity}' % fname)
        exit(2)
    origin = Origin()
    origin.time = UTCDateTime( xyzt_array[3])
    origin.x = xyzt_array[0]
    origin.y = xyzt_array[1]
    origin.z = xyzt_array[2]
    event = Event()
    event.origins = [origin]
    # Don't use the method below - it bungles the pref id
    #event.preferred_origin_id = ResourceIdentifier(referred_object=origin)
    # Either of these methods seems to work:
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id)
    #event.preferred_origin_id = origin.resource_id
    origin.method = 'InterLoc Event'

    logger.info('%s: Start from InterLoc Origin:' % fname)
    logger.info(origin)
    event.write('event.xml', format='quakeml')
    logger.info('%s: Write InterLoc Origin to event.xml' % fname)

    starttime = origin.time - 0.1
    endtime   = origin.time + 0.9
    logger.debug('%s: call get_stream_from_mongo(starttime=[%s] endtime=[%s]' % (fname, starttime, endtime))
    st1 = get_stream_from_mongo(starttime, endtime)
    if st1 is None:
        logger.info('%s: get_stream_from_mongo returned None --> return' % fname)
        return

    for tr in st1:
        logger.info('tr:%3s.%s %s - %s n:%d sr:%f d:%s e:%s' % (tr.stats.station, tr.stats.channel, \
                   tr.stats.starttime, tr.stats.endtime, tr.stats.npts, tr.stats.sampling_rate, \
                   tr.data.dtype, tr.stats.mseed.encoding))

    # ATODO MTH: get_stream() is returning obspy Stream, not microquake
    st = Stream(st1.copy())
    clean_nans(st, threshold=.05)
    st.filter("bandpass", freqmin=80.0, freqmax=600.0, corners=4)
    # filter will return all np data arrays as float64 which no longer matches mseed.encoding:
    for tr in st:
        tr.stats.mseed.encoding = 'FLOAT64'

    title = 'InterLoc Orig <%.0f, %.0f, %.0f> %s' % (origin.x, origin.y, origin.z, origin.time)

    st.write("event.mseed")
    if plot_profiles:
        plot_profile_with_picks(st, picks=None, origin=origin, title=title)

    interloc_origin = copy.deepcopy(origin)

  # 1. Stack traces to get origin time + prelim predicted picks for origin time + loc:
    logger.info('%s: stack traces to get new origin time' % fname)
    event2  = core.create_event(st, origin.loc)[0]
    logger.info('%s: stack traces to get new origin time [DONE]' % fname)
    origin = event2.origins[0]
    event.origins.append(copy.deepcopy(origin))
    event.picks = copy.deepcopy(event2.picks)
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id)
    event.write('event2.xml', format='quakeml')

    noisy_chans = check_trace_channels_with_picks(st, copy_picks_to_dict(event2.picks))
# Keep copy of original stream with all traces
    st2 = st.copy()
    for tr in st:
        if tr in noisy_chans:
            #logger.warn("Remove noisy trace:%s" % tr.get_id())
            st.remove(tr)

    title = 'create_event %s' % (origin.time)
    if plot_profiles:
        plot_profile_with_picks(st, picks=event2.picks, origin=origin, title=title + ' prelim picks, clean ch')

 # 2. Repick:
    logger.info("%s: Repick with SNR_picker" % fname)
    p_snr_picks = SNR_picker(st, event2.picks, SNR_dt=np.linspace(-.08, .05, 200), SNR_window=(10e-3, 5e-3),  filter='P')
    s_snr_picks = SNR_picker(st, event2.picks, SNR_dt=np.linspace(-.05, .05, 200), SNR_window=(20e-3, 10e-3), filter='S')

    snr_picks = p_snr_picks + s_snr_picks

    if plot_profiles:
        plot_profile_with_picks(st, picks=snr_picks, origin=origin, \
                                title=origin.time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " [SNR picks]")

    logger.info("Before clean_picks: SNR picks: n=%d" % len(snr_picks))
  # 3. Clean up picks:
    noisy_picks = clean_picks(st, snr_picks, preWl=.03, postWl=.03, thresh_P=5.9, thresh_S=3.7, debug=False)
    cleaned_picks = [pick for pick in snr_picks if pick not in noisy_picks]

    for pick in noisy_picks:
        logger.debug("%s: Remove noisy pick from snr picks: sta:%3s [%s]" % \
                    (fname, pick.waveform_id.station_code, pick.phase_hint))
    '''
        snr_picks.remove(pick)
    logger.info(" After clean_picks: SNR picks: n=%d" % len(snr_picks))
    '''

    if plot_profiles:
        plot_profile_with_picks(st, picks=cleaned_picks, origin=origin, \
                                title=origin.time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " [Cleaned SNR picks]")

 # 4. Make the context mseed
    sorted_p_picks = sorted([pick for pick in cleaned_picks if pick.phase_hint == 'P'], key=lambda x: x.time)
    first_pick = sorted_p_picks[0]
    first_sta  = first_pick.waveform_id.station_code
    logger.info("%s: Make context mseed for first_sta:%s" % (fname, first_sta))
    starttime = origin.time - 10
    endtime   = origin.time + 10
    st_temp   = get_stream_from_mongo(starttime, endtime, sta=first_sta)
    st_new = st_temp.composite()
    #st_new.plot()
    st_new.write("event_context.mseed")


    event.picks += copy.deepcopy(cleaned_picks)
    event.origins[1].arrivals = picks_to_arrivals(cleaned_picks) 
    #event.preferred_origin_id = event.origins[1].resource_id
    event.write('event3.xml', format='quakeml')

    min_number_picks = 10
    if len(cleaned_picks) < min_number_picks:
        logger.warn('%s: n_cleaned_picks=%d < min_number_picks (%d) --> ** STOP **' % \
                   (fname, len(cleaned_picks), min_number_picks))
        return None


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

  # 5. Relocate event with cleaned picks

    config_file = config_dir + '/project.xml'
    params = ctl.parse_control_file(config_file)
    nll_opts = nlloc.init_nlloc_from_params(params)

    # The following line only needs to be run once. It creates the base directory
    # MTH: this will re-create the station time grids in spp common/NLL/time:
    #nll_opts.prepare(create_time_grids=True, tar_files=False, SparkContext=None)
    #exit()

# Need to pass in an event with a preferred origin + arrivals
    import datetime

    now = datetime.datetime.now()
    logger.info('%s: Call NLLOC time:%s' % (fname, now))
    event_new = nll_opts.run_event(event)[0]
    logger.info('%s: Call NLLOC time:%s [DONE]' % (fname, datetime.datetime.now()))

    if len(event.origins) < 2:
        logger.warn('%s: Seems NLLOC run_event failed !!' % fname)

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

    logger.info('%s: Finished processing event (id=%s) --> Now clean up!' % (fname, event_id))
    exit()

    from glob import glob
    file_list = glob('event*.xml')
    for f in file_list:
        logger.debug('%s: remove file:%s' % (fname, f))
        os.remove(f) 
    file_list = glob('event*.mseed')
    for f in file_list:
        logger.debug('%s: remove file:%s' % (fname, f))
        os.remove(f) 

    logger.info('%s: Clean up DONE --> on to next event' % (fname))

    return

    #exit()

    '''
    import pickle as pickle
    #qbytes = pickle.dumps(cat_read)
    qbytes = pickle.dumps(cat_new)
    cat_2 = pickle.loads(qbytes)
    print('   cat_2: %s' % cat_2.preferred_origin().resource_id.id)
    cat_2.write('foo2.xml', format='quakeml')
    exit()
    '''


  # 6. Compare resid between cleaned picks and predicted picks for nlloc origin:
    resid = calculate_residual(st, cleaned_picks, ot_event.preferred_origin()) 

# Does nlloc return new picks ??
    plot_profile_with_picks(st, picks=None, origin=ot_event.preferred_origin(), title='2018-04-14 03:44:22 [NLLOC origin]')

if __name__ == "__main__":
    main()

