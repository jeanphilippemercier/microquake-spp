
from helpers import *
import matplotlib.pyplot as plt

import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from spp.utils.application import Application
from microquake.core import read
from microquake.core.event import read_events as read_events
from obspy.core.event import read_events as obs_read_events
from obspy.core import UTCDateTime
from microquake.core.event import (Origin, CreationInfo, Event)
from microquake.core import UTCDateTime

from microquake.core.data.station2 import get_inventory

from microquake.waveform.mag2 import moment_magnitude, measure_pick_smom
from obspy.core.event.base import ResourceIdentifier

from microquake.waveform.amp_measures import measure_pick_amps, set_pick_snrs
from microquake.waveform.mag_utils import inv_station_list_to_dict

from microquake.waveform.pick import calculate_snr

def main():

    # reading application data
    app = Application()
    settings = app.settings
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    event_file = data_dir + "20180706112101.xml"
    #event_file = data_dir + "20180628153305.xml"
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
    stations = get_inventory(sensor_csv)[0]

    sta_meta_dict = inv_station_list_to_dict(stations)

    #plot_channels_with_picks(st, sta, event.picks, channel='z', title="IMS picks")

# Re-pick with snr picker and only look at polarity for traces with pick snr > thresh
    from repick import picker

    snr_picks = picker(st, event, extra_msgs=None, logger=None, params=params, app=None)

    #plot_channels_with_picks(st, sta, snr_picks, channel='z', title="SNR picks")

    # TODO: The above will have the effect of removing many (mostly S) picks
    #       Some of these should be salvaged, so either need to tune snr_picker
    #       and/or remove noisy traces before passing composite to snr_picker
    # Some of the low SNR S picks look a bit late

    # The SNR picker produces an SNR for each *composite trace* pick, but we want an
    #   SNR for each trace at the pick time
    picks = snr_picks
    pick_dict = copy_picks_to_dict(picks)
    set_pick_snrs(st, picks, pre_wl=.03, post_wl=.03)


# 1. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces

    measure_pick_amps(st, picks, debug=False)

    for tr in st:
        sta = tr.stats.station
        for pha in ['P']:
            key = "%s_arrival" % pha
            if key in tr.stats:

                if 'velocity_pulse' in tr.stats[key]:
                    pulse_dict = tr.stats[key]['velocity_pulse']
                    polarity = pulse_dict['polarity']
                    t1 = pulse_dict['t1']
                    t2 = pulse_dict['t2']
                    tpeak = pulse_dict['tpeak']
                    peak_vel = pulse_dict['peak_vel']
                    pulse_snr = pulse_dict['pulse_snr']

                    print("%s: Vel Peak [%s]: pulse_snr:%.1f pulse_width:%.4f [Polarity:%d]" % \
                         (tr.get_id(), pha, pulse_snr, t2 - t1, polarity))
                    print("  pick_t:%s" % (pick_dict[sta]['P'].time))
                    print("      t1:%s" % (t1))
                    print("   tpeak:%s peak:%g" % (tpeak,peak_vel))
                    print("      t2:%s" % (t2))
                    print()


    print("===============================================\n")
    print("ev_loc: x:%.1f y:%.1f z:%.1f" % (ev_loc[0], ev_loc[1], ev_loc[2]))

    for tr in st:
        sta=tr.stats.station
        cha=tr.stats.channel

        if sta not in pick_dict:
            print("sta:%s missing from pick_dict --> Skip" % sta)
            continue

        if cha == 'z' and 'velocity_pulse' in tr.stats.P_arrival:
            st_loc = sta_meta_dict[sta]['station'].loc

            polarity = tr.stats.P_arrival.velocity_pulse.polarity

            #st_loc[0] = ev_loc[0] + 1000
            #st_loc[1] = ev_loc[1] + 1000

            dy = st_loc[1] - ev_loc[1]
            dx = st_loc[0] - ev_loc[0]

            az = np.arctan2(dx,dy) * 180./np.pi

            print("sta:%3s cha:%s x:%8.1f y:%8.1f az:%7.2f polarity:%d" % \
                 (sta, cha, dx, dy, az, polarity))
            #if tr.stats.P_arrival.polarity > 0:
                #plot_channels_with_picks(st, sta, event.picks, title="sta:%s cha:%s polarity=+1" % (sta,cha))
            #plot_channels_with_picks(st, sta, event.picks, title="sta:%s cha:%s polarity=+1" % (sta,cha))
            #if polarity > 0: 
                #tr.plot()

if __name__ == '__main__':

    main()
