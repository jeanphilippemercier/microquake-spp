
import os

import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from spp.utils.application import Application
from microquake.core import read
from microquake.core.event import read_events
from microquake.core.event import (Origin, CreationInfo, Event)
from microquake.core import UTCDateTime

from microquake.core.data.station2 import get_inventory, inv_station_list_to_dict

from microquake.waveform.amp_measures import measure_pick_amps, set_pick_snrs
from microquake.waveform.mag_new import calc_magnitudes_from_lambda, set_new_event_mag, moment_magnitude_new
from microquake.waveform.smom_mag import measure_pick_smom

from obspy.core.event.base import ResourceIdentifier

from helpers import *

def main():

    fname = 'test_moment_mag'

    # reading application data
    app = Application()
    settings = app.settings
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    event_file = data_dir + "20180706112101.xml"
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

    #plot_profile_with_picks(st, picks=event.picks, origin=origin, title="IMS picks")
    sensor_csv = os.environ['SPP_COMMON'] + '/sensors.csv'
    inventories = get_inventory(sensor_csv)
    st.attach_response(inventories)

    noisy_channels = remove_noisy_traces(st, event.picks)
    for tr in noisy_channels:
        print("%s: Removed noisy tr:%s" % (fname, tr.get_id()))


#  We can calculate moment_magnitude in one line, but it will use the event.picks
#    to window the displacement pulse, etc.

    """
    moment_magnitude_new(st, event, inventories[0], vp=vp, vs=vs, density=2700, use_smom=False)

    for mag in event.magnitudes:
        print(mag)
    """

# Or we can drive the underlying functions ourselves:

# 0. Re-pick with snr picker and only look at polarity for traces with pick snr > thresh
    from repick import picker

# MTH: Obspy has some bugs. If we don't make touch the reference to the picks pointed to in arrivals
#      here, then it loses them later when we need them (e.g., we often use pick.phase_hint)
    arrivals = event.preferred_origin().arrivals
    for arr in arrivals:
        pk = arr.pick_id.get_referred_object()
        #print("Before: type=%s [%s]" % (type(pk), pk.phase_hint))

    snr_picks = picker(st, event, extra_msgs=None, logger=None, params=params, app=None)

    set_pick_snrs(st, snr_picks, pre_wl=.03, post_wl=.03)

# use_smom: True = Calc moment mag in the frequency domain

# 1. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
    use_smom = False

    #use_smom = True
    if use_smom:
        origin = event.preferred_origin()
        synthetic_picks = app.synthetic_arrival_times(origin.loc, origin.time)
        measure_pick_smom(st, inventories[0], event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='P', debug=False)
        measure_pick_smom(st, inventories[0], event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='S', debug=False)
        comment="Average of freq-domain P & S moment magnitudes"
    else:
        measure_pick_amps(st, snr_picks, debug=False)
        comment="Average of time-domain P & S moment magnitudes"

# 2. Use time(or freq) lambda to calculate moment magnitudes for each arrival

    Mw_P, station_mags_P = calc_magnitudes_from_lambda(st, event, inventories[0], vp=vp, vs=vs,
                                                       density=2700, P_or_S='P', use_smom=use_smom)

    Mw_S, station_mags_S = calc_magnitudes_from_lambda(st, event, inventories[0], vp=vp, vs=vs,
                                                       density=2700, P_or_S='S', use_smom=use_smom)

    print("In main: Mw_P=%.1f [from disp_area]" % Mw_P)
    print("In main: Mw_S=%.1f [from disp_area]" % Mw_S)

# 3. Average Mw_P,Mw_S to get event Mw and wrap with list of station mags/contributions

    Mw = 0.5 * (Mw_P + Mw_S)

    station_mags = station_mags_P + station_mags_S
    set_new_event_mag(event, station_mags, Mw, comment)

    for mag in event.magnitudes:
        print(mag)



if __name__ == '__main__':

    main()
