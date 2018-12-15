
from helpers import *
import matplotlib.pyplot as plt

import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from spp.utils.application import Application
from microquake.core import read
from microquake.core.event import read_events as read_events
from obspy.core.event import read_events as obs_read_events
from microquake.core.event import (Origin, CreationInfo, Event)
from microquake.core import UTCDateTime

from microquake.core.data.station2 import get_inventory

from microquake.waveform.mag2 import moment_magnitude, measure_pick_smom
from obspy.core.event.base import ResourceIdentifier

from microquake.waveform.amp_measures import measure_pick_amps
from microquake.waveform.mag_utils import inv_station_list_to_dict

from test_moment_mag_time import calc_magnitudes_from_lambda

def main():

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
    #print(vp,vs)

    #plot_profile_with_picks(st, picks=event.picks, origin=origin, title="IMS picks")
    sensor_csv = os.environ['SPP_COMMON'] + '/sensors.csv'
    stations = get_inventory(sensor_csv)[0]

    synthetic_picks = app.synthetic_arrival_times(origin.loc, origin.time)

    from microquake.core.util.tools import copy_picks_to_dict
    from microquake.waveform.pick import calculate_snr
    from microquake.core.stream import Stream

    measure_pick_smom(st, stations, event, synthetic_picks, fmin=20., fmax=1e3,
                      P_or_S='P', debug=False)
    measure_pick_smom(st, stations, event, synthetic_picks, fmin=20., fmax=1e3,
                      P_or_S='S', debug=False)
    # This is likely temporary and can be removed once we are working
    # with automatic picks that already have snr set
    pick_dict = copy_picks_to_dict(event.picks)
    for phase in ['P', 'S']:
        if 'snr' not in tr.stats[key]:
            key = '%s_arrival' % phase
            for tr in st:
                pick_time = pick_dict[tr.stats.station][phase].time
                tr.stats[key]['snr'] = calculate_snr(Stream(traces=[tr]), pick_time, pre_wl=.03, post_wl=.03)

    Mw_P, station_mags_P = calc_magnitudes_from_lambda(st, event, stations, vp=vp, vs=vs,
                                                          density=2700, P_or_S='P', use_smom=True)

    Mw_S, station_mags_S = calc_magnitudes_from_lambda(st, event, stations, vp=vp, vs=vs,
                                                          density=2700, P_or_S='S', use_smom=True)
    print("In main: Mw_P=%.1f [from smom]" % Mw_P)
    print("In main: Mw_S=%.1f [from smom]" % Mw_S)


# 3. Average Mw_P,Mw_S to get event Mw and wrap with list of station mags/contributions

    Mw = 0.5 * (Mw_P + Mw_S)

    station_mags = station_mags_P + station_mags_S
    count = len(station_mags)

    sta_mag_contributions = []
    for sta_mag in station_mags:
        sta_mag_contributions.append( StationMagnitudeContribution(
                                        station_magnitude_id=sta_mag.resource_id)
                                    )

    origin_id = event.preferred_origin().resource_id

    event_mag = Magnitude(origin_id=origin_id,
                          mag=Mw,
                          magnitude_type='Mw',
                          station_count=count,
                          evaluation_mode='automatic',
                          station_magnitude_contributions=sta_mag_contributions,
                          comments=[Comment(text="Average of time-domain P & S moment magnitudes")],
                         )

    event.magnitudes.append(event_mag)
    event.station_magnitudes = station_mags

    for mag in event.magnitudes:
        print(mag)

    return


if __name__ == '__main__':

    main()
