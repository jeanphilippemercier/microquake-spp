
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

    #plot_profile_with_picks(st, picks=event.picks, origin=origin, title="IMS picks")
    sensor_csv = os.environ['SPP_COMMON'] + '/sensors.csv'
    stations = get_inventory(sensor_csv)[0]

    sta_meta_dict = inv_station_list_to_dict(stations)

# 1. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces

    measure_pick_amps(st, event.picks, debug=False)

    print("ev_loc: x:%.1f y:%.1f" % (ev_loc[0], ev_loc[1]))

    for tr in st:
        sta=tr.stats.station
        cha=tr.stats.channel
        if cha == 'z':
            st_loc = sta_meta_dict[sta]['station'].loc

            #st_loc[0] = ev_loc[0] + 1000
            #st_loc[1] = ev_loc[1] + 1000

            dy = st_loc[1] - ev_loc[1]
            dx = st_loc[0] - ev_loc[0]

            az = np.arctan2(dx,dy) * 180./np.pi

            print("sta:%3s cha:%s x:%8.1f y:%8.1f az:%7.2f polarity:%d" % \
                 (sta, cha, dx, dy, az, tr.stats.P_arrival.polarity))
            if tr.stats.P_arrival.polarity > 0:
                plot_channels_with_picks(st, sta, event.picks, title="sta:%s cha:%s polarity=+1" % (sta,cha))

if __name__ == '__main__':

    main()
