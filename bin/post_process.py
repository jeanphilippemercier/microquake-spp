
from obspy.core.event.base import ResourceIdentifier

from microquake.core import read
from microquake.core import UTCDateTime
from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.core.event import read_events as read_events

from spp.utils.application import Application

from lib_process import fix_arr_takeoff_and_azimuth

from spp.utils.seismic_client import RequestEvent, get_events_catalog, get_event_by_id

import os
import argparse

import logging
fname = 'post_process'
logger = logging.getLogger(fname)

from lib_process import processCmdLine


def main():

    use_web_api, xml_out, xml_in, mseed_in = processCmdLine(fname)

    # reading application data
    app = Application()
    settings = app.settings

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()

    if use_web_api:
        logger.info("Read from web_api")
        api_base_url = settings.seismic_api.base_url
        start_time = UTCDateTime("2018-07-06T11:21:00")
        end_time = start_time + 3600.
        request = get_events_catalog(api_base_url, start_time, end_time)
        cat = request[0].get_event()
        event=cat[0]
        print(event)
        #print(event.resource_id)
        #print(request[0].event_resource_id)
        st = request[0].get_waveforms()
        #for tr in st:
            #print(tr.get_id())

        '''
        event_id = "smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752"
        request = get_event_by_id(api_base_url, event_id)
        print(type(request))
        print(request)
        exit()
        '''
    else:
        logger.info("Read from files on disk")
        st = read(mseed_in, format='MSEED')
        # Fix 4..Z channel name:
        #for tr in st:
            #tr.stats.channel = tr.stats.channel.lower()
            #print(tr.get_id())

        # Fix broken preferred:
        event  = read_events(xml_in)[0]
        origin = event.origins[0]
        event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id, referred_object=origin)
        mag = event.magnitudes[0]
        event.preferred_magnitude_id = ResourceIdentifier(id=mag.resource_id.id, referred_object=mag)

    """
    data_dir   = '/Users/mth/mth/Data/OT_data/'
    event_file = data_dir + "20180628153305.xml"
    event_file = data_dir + "20180609195044.xml"
    event_file = data_dir + "20180523111608.xml"
    event_file = data_dir + "20180706112101.xml"
    mseed_file = event_file.replace('xml','mseed')
    st = read(mseed_file, format='MSEED')
    """


    ev_loc = event.preferred_origin().loc

    vp_grid, vs_grid = app.get_velocities()
    vp = vp_grid.interpolate(ev_loc)[0]
    vs = vs_grid.interpolate(ev_loc)[0]

    inventory = app.get_inventory()
    st.attach_response(inventory)

    sta_meta_dict = inv_station_list_to_dict(inventory)

    #noisy_channels = remove_noisy_traces(st, event.picks)
    #for tr in noisy_channels:
        #print("%s: Removed noisy tr:%s" % (fname, tr.get_id()))


# 2. Repick
    #from zlibs import picker
    picker = __import__('033_picker').picker
    params = app.settings.picker
    # This will create a new (2nd) origin with origin.time from stacking and origin.loc same as original orogin.loc
    #  The new origin will have arrivals for each snr pick that exceeded snr_threshold
    # Both event.preferred_origin and cat_out[0].preferred_origin will be set to this new (2nd) origin
    #cat_out, st_out = picker(cat=[event], stream=st, extra_msgs=None, logger=logger, params=params, app=app)
    cat_out, st_out = picker(cat=[event], stream=st, extra_msgs=None, logger=logger, params=params, app=app)

    snr_picks = [ pk for pk in cat_out[0].picks if pk.method is not None and 'snr_picker' in pk.method ]

# 3. Relocate
    #from zlibs import location
    location = __import__('044_hypocenter_location').location
    from microquake.nlloc import NLL, calculate_uncertainty
    params = app.settings.nlloc
    logger.info('Preparing NonLinLoc')
    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar, sensors=sensors, params=params)

    # This will create a new (3rd) origin and will set cat_out[0].preferred_origin to point to it,
    #   however, event will still contain only 2 origins and event.preferred_origin points to the old preferred

    cat_out, st_out = location(cat=[event], stream=st, extra_msgs=None, logger=logger, nll=nll,
                               params=params, project_code=project_code, app=app)

    origin = cat_out[0].preferred_origin()
    logger.info("NLLoc locn:<%.1f %.1f %.1f>" % (origin.loc[0], origin.loc[1], origin.loc[2]))
    logger.info(origin)

# 4. Fix nlloc origin.arrival angles:

    # At this point the arrival take_off angles are incorrect - they would have been read directly from 
    #  the last.hyp nlloc output where they were in turn read/interpolated from common/time/OT.41.P.angle.buf
    #  produced by Grid2Time with ANGLES_YES set.  These angles look wrong - I think because our positive vertical
    #  is opposite (?) the NLLOC convention, hence station wrt event depth is wrong (?)
    #
    #  The newly calculated distances are hypocentral (ray) and are exactly the same as those in nlloc last.hypo
    #    however, the new arrival.time_residual are DIFFERENT than nlloc values!

    fix_arr_takeoff_and_azimuth(cat_out, sta_meta_dict, app=app)

    # Just to reinforce that these are hypocentral distance in meters ... to be used by moment_mag calc
    # ie, obspy.arrival.distance = epicenteral distance in degrees
    origin = cat_out[0].preferred_origin()
    for arr in origin.arrivals:
        arr.hypo_dist_in_m = arr.distance

    cat_out.write(xml_out, format='QUAKEML')

    return

if __name__ == '__main__':

    main()
