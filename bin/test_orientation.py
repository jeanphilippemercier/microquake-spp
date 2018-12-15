
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

from microquake.waveform.mag2 import moment_magnitude
from obspy.core.event.base import ResourceIdentifier

#from microquake.waveform.polarity import get_polarity

def main():

    # reading application data
    app = Application()
    settings = app.settings
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)

    # htt = app.get_ttable_h5()

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    event_file = data_dir + "20180706112101.xml"
    #event_file = data_dir + "20180628153305.xml"
    mseed_file = event_file.replace('xml','mseed')
    st = read(mseed_file, format='MSEED')
    #st.detrend('demean').detrend('linear')
    #st.filter('bandpass', freqmin=10., freqmax=3000).taper(type='cosine', max_percentage=0.05, side='both')
    #st.integrate().detrend('demean').detrend('linear')

    event  = read_events(event_file)[0]
    origin = event.origins[0]
    # Fix broken preferred:
    event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id, referred_object=origin)
    mag = event.magnitudes[0]
    event.preferred_magnitude_id = ResourceIdentifier(id=mag.resource_id.id, referred_object=mag)

    #plot_profile_with_picks(st, picks=event.picks, origin=origin, title="IMS picks")
    sensor_csv = os.environ['SPP_COMMON'] + '/sensors.csv'
    stations = get_inventory(sensor_csv)[0]
    for sta in stations:
        if len(sta.channels) < 3:
            print("Skip station:%s nchan < 3" % sta.code)
            continue
        ch  = sta.channels
        ch0 = np.array([ch[0].cos1,ch[0].cos2,ch[0].cos3])
        ch1 = np.array([ch[1].cos1,ch[1].cos2,ch[1].cos3])
        ch2 = np.array([ch[2].cos1,ch[2].cos2,ch[2].cos3])
        #print(np.dot(ch0,ch0), np.dot(ch1,ch1), np.dot(ch2,ch2))
        print(sta.code, np.dot(ch0,ch1), np.dot(ch0,ch2), np.dot(ch1,ch2))

        '''
        print("sta:%s 0:%s 1:%s 2:%s" % (sta.code,ch[0].code, ch[1].code, ch[2].code))
        dot01 = np.sum(ch[0].cos1*ch[1].cos1 + ch[0].cos2*ch[1].cos2 + ch[0].cos3*ch[1].cos3)
        print(np.sum(ch[0].cos1*ch[1].cos1 + ch[0].cos2*ch[1].cos2 + ch[0].cos3*ch[1].cos3))
        print(np.sum(ch[0].cos1*ch[2].cos1 + ch[0].cos2*ch[2].cos2 + ch[0].cos3*ch[2].cos3))
        print(np.sum(ch[1].cos1*ch[2].cos1 + ch[1].cos2*ch[2].cos2 + ch[1].cos3*ch[2].cos3))
        print("sta:%s 0:%s 1:%s 2:%s" % (sta.code,ch[0].code, ch[1].code, ch[2].code))
        '''
        '''
        for ch in station.channels:
            #print(ch)
            mag = (np.sqrt( ch.cos1**2 + ch.cos2**2 + ch.cos3**2))
            print("sta:%s ch:%s cos1:%.4f cos2:%.4f cos3:%.4f mag:%.4f" % \
                  (station.code, ch.code, ch.cos1, ch.cos2, ch.cos3, mag))
            #exit()
        '''
    exit()
    get_polarity(st, event, stations)



if __name__ == '__main__':

    main()
