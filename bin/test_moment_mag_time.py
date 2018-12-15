
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

# 2. Use displacement areas to calculate time-domain moment magnitudes for each arrival

    Mw_P, station_mags_P = calc_magnitudes_from_lambda(st, event, stations, vp=vp, vs=vs,
                                                          density=2700, P_or_S='P')
    Mw_S, station_mags_S = calc_magnitudes_from_lambda(st, event, stations, vp=vp, vs=vs,
                                                          density=2700, P_or_S='S')

    print("In main: Mw_P=%.1f [from disp_area]" % Mw_P)
    print("In main: Mw_S=%.1f [from disp_area]" % Mw_S)

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


from obspy.core.event.magnitude import Magnitude, StationMagnitude, StationMagnitudeContribution
from obspy.core.event.base import Comment, WaveformStreamID

def calc_magnitudes_from_lambda(st, event, stations, vp=5300, vs=3500, density=2700, P_or_S='P', use_smom=False):
    """
    Calculate the moment magnitude at each station from lambda, where lambda is either:
        'dis_pulse_area' (use_smom=False) - calculated by integrating arrival displacement pulse in time
        'smom' (use_smom=True) - calculated by fiting Brune spectrum to displacement spectrum in frequency
    """

    sta_meta_dict = inv_station_list_to_dict(stations)

    origin = event.preferred_origin()
    ev_loc = origin.loc
    origin_id = origin.resource_id

    rad_P, rad_S = 0.52, 0.63

    if P_or_S == 'P':
        arrival_dict = 'P_arrival'
        v = vp
        rad = rad_P
        mag_type = 'Mw_P'
    else:
        arrival_dict = 'S_arrival'
        v = vs
        rad = rad_S
        mag_type = 'Mw_S'

    magnitude_comment = 'moment magnitude calculated from displacement pulse area ' 
    if use_smom:
        lambda_key = 'smom'
        magnitude_comment+= 'measured in frequeny-domain (smom)'
    else:
        lambda_key = 'dis_pulse_area'
        magnitude_comment+= 'measured in time-domain'


    M0_scale = 4. * np.pi * density * v**3 / rad

    # Loop over unique stations in stream
    # If there are 3 chans and ...
    station_mags = []
    Mw_list = []

    Mw_P = []
    snr_thresh = 8.
    for sta in sorted([sta for sta in st.unique_stations()],
                    key=lambda x: int(x)):

        trs = st.select(station=sta)

        sta_dict = sta_meta_dict[sta]

        nchans_expected = sta_dict['nchans']
        nchans_found = len(trs)

        #print("sta:%s nchan_expected:%d nfound:%d" % \
            #(sta, nchans_expected, nchans_found))

        # Only process tri-axial sensors (?)
        if nchans_expected != 3:
            continue

        #print("Process sta:%s" % sta)

        vector_sum = 0
        nused = 0

        for i,tr in enumerate(trs):

            if arrival_dict in tr.stats and lambda_key in tr.stats[arrival_dict] :

                snr  = tr.stats[arrival_dict]['snr']

                if snr >= snr_thresh:
                    vector_sum += tr.stats[arrival_dict][lambda_key] ** 2.
                    nused += 1
                else:
                    print("  Drop tr:%s snr:%.1f < snr_thresh(%.1f)" % (tr.get_id(), snr, snr_thresh))


                R  = np.linalg.norm(sta_dict['station'].loc -ev_loc) # Dist in meters
                M0 = M0_scale * R * np.abs(tr.stats[arrival_dict][lambda_key])
                equiv_Mw = 2./3. * np.log10(M0) - 6.033
                Mw_P.append(equiv_Mw)
                print("sta:%s cha:%s snr:%.1f %s[%s]=%12.10g equiv_Mw:%.2f" % \
                      (sta, tr.stats.channel, snr, arrival_dict, \
                       lambda_key,tr.stats[arrival_dict][lambda_key], equiv_Mw))

        if nused > 0:
            vector_sum = np.sqrt(vector_sum)
            R  = np.linalg.norm(sta_dict['station'].loc -ev_loc) # Dist in meters

            M0 = M0_scale * R * vector_sum
            Mw = 2./3. * np.log10(M0) - 6.033

            print("sta:%s [%s] R:%5.1f vector Mw:%5.2f [nused=%d]" % (sta, P_or_S, R, Mw, nused))
            Mw_list.append(Mw)

            station_mag = StationMagnitude(origin_id=origin_id, mag=Mw,
                            station_magnitude_type=mag_type,
                            comments=[Comment(text=magnitude_comment)],
                            waveform_id=WaveformStreamID(
                                        network_code=tr.stats.network,
                                        station_code=tr.stats.station,
                                        channel_code=tr.stats.channel,
                                        ),
                          )
            station_mags.append(station_mag)

    else:
        print(">> Skip 1-C sta:%s" % sta)


    print("nmags=%d avg:%.1f med:%.1f std:%.1f" % \
          (len(Mw_list), np.mean(Mw_list), np.median(Mw_list), np.std(Mw_list)))

    print("Equiv Mw_%s: nchans=%d mean=%.2f median=%.2f std=%.3f" % (P_or_S, len(Mw_P), np.mean(Mw_P), \
                                                           np.median(Mw_P), np.std(Mw_P)))

    return np.median(Mw_list), station_mags


if __name__ == '__main__':

    main()
