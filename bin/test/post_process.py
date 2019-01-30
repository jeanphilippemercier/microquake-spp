
from helpers import *
import matplotlib.pyplot as plt

import os
import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from obspy.core.event.base import ResourceIdentifier

from microquake.core import read
from obspy.core.stream import read as obs_read
from microquake.core import UTCDateTime
from microquake.core.event import read_events as read_events
from microquake.core.event import (Origin, CreationInfo, Event)
from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.waveform.amp_measures import measure_pick_amps, measure_velocity_pulse, set_pick_snrs, measure_displacement_pulse

from microquake.core.util.tools import copy_picks_to_dict

from spp.utils.application import Application

from lib_process import *


def main():

    fname = 'post_process'

    # reading application data
    app = Application()
    settings = app.settings

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()

    logger = app.get_logger('test_xml','test_xml.log')

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    #event_file = data_dir + "20180706112101.xml"
    event_file = data_dir + "20180628153305.xml"
    event_file = data_dir + "20180609195044.xml"
    event_file = data_dir + "20180523111608.xml"
    mseed_file = event_file.replace('xml','mseed')
    st = read(mseed_file, format='MSEED')

    # Fix 4..Z channel name:
    for tr in st:
        tr.stats.channel = tr.stats.channel.lower()

    event  = read_events(event_file)[0]
    origin = event.origins[0]
    # Fix broken preferred:
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id, referred_object=origin)
    mag = event.magnitudes[0]
    event.preferred_magnitude_id = ResourceIdentifier(id=mag.resource_id.id, referred_object=mag)
    ev_loc = event.preferred_origin().loc

    vp_grid, vs_grid = app.get_velocities()
    vp = vp_grid.interpolate(ev_loc)[0]
    vs = vs_grid.interpolate(ev_loc)[0]

    inventory = app.get_inventory()
    st.attach_response(inventory)

    sta_meta_dict = inv_station_list_to_dict(inventory)

# 1. Rotate to ENZ:
    st_rot = rotate_to_ENZ(st, sta_meta_dict)
    st = st_rot

    noisy_channels = remove_noisy_traces(st, event.picks)
    for tr in noisy_channels:
        print("%s: Removed noisy tr:%s" % (fname, tr.get_id()))


# 2. Repick
    from zlibs import picker
    params = app.settings.picker
    # This will create a new (2nd) origin with origin.time from stacking and origin.loc same as original orogin.loc
    #  The new origin will have arrivals for each snr pick that exceeded snr_threshold
    # Both event.preferred_origin and cat_out[0].preferred_origin will be set to this new (2nd) origin
    #cat_out, st_out = picker(cat=[event], stream=st, extra_msgs=None, logger=logger, params=params, app=app)
    cat_out, st_out = picker(cat=[event], stream=st, extra_msgs=None, logger=logger, params=params, app=app)

    snr_picks = [ pk for pk in cat_out[0].picks if pk.method is not None and 'snr_picker' in pk.method ]

# 3. Relocate
    from zlibs import location
    from microquake.nlloc import NLL, calculate_uncertainty
    params = app.settings.nlloc
    logger.info('Preparing NonLinLoc')
    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar, sensors=sensors, params=params)

    # This will create a new (3rd) origin and will set cat_out[0].preferred_origin to point to it,
    #   however, event will still contain only 2 origins and event.preferred_origin points to the old preferred

    cat_out, st_out = location(cat=[event], stream=st, extra_msgs=None, logger=logger, nll=nll,
                               params=params, project_code=project_code)

    origin = cat_out[0].preferred_origin()
    print("NLLoc locn:<%.1f %.1f %.1f>" % (origin.loc[0], origin.loc[1], origin.loc[2]))
    print(origin)

# 4. Fix nlloc origin.arrival angles:

    # At this point the arrival take_off angles are incorrect - they would have been read directly from 
    #  the last.hyp nlloc output where they were in turn read/interpolated from common/time/OT.41.P.angle.buf
    #  produced by Grid2Time with ANGLES_YES set.  These angles look wrong - I think because our positive vertical
    #  is opposite (?) the NLLOC convention, hence station wrt event depth is wrong (?)

    fix_arr_takeoff_and_azimuth(cat_out, sta_meta_dict, app=app)

    cat_out.write("event.xml", format='QUAKEML')

    '''
    pick_dict = copy_picks_to_dict(snr_picks)
    for arr in cat_out[0].preferred_origin().arrivals:
        pk = arr.pick_id.get_referred_object()
        sta = pk.waveform_id.station_code
        cha = pk.waveform_id.channel_code
        print("sta:%s cha:%s pha:%s time:%s [%s] az:%.1f th:%.1f dist:%.1f" % \
              (sta, cha, pk.phase_hint, pk.time, pick_dict[sta][pk.phase_hint].time, \
               arr.azimuth, arr.takeoff_angle, arr.distance))
    '''

    return

if __name__ == '__main__':

    main()
