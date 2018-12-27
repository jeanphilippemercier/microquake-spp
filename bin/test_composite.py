
from helpers import *
import matplotlib.pyplot as plt

import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from spp.utils.application import Application
from microquake.core import read
from microquake.core.event import read_events as read_events

def main():

    # reading application data
    app = Application()

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    event_file = data_dir + "20180706112101.xml"
    mseed_file = event_file.replace('xml','mseed')

    event  = read_events(event_file)[0]
    origin = event.origins[0]
    picks  = event.picks

    st = read(mseed_file, format='MSEED')

    sta = '100'
    st_c = st.composite()
    tr1 = st_c.select(station = sta)[0]
    tr1.stats.channel='tr1'

    tr11 = st_c.select(station = sta)

    tr2 = st.select(station = sta).composite()[0]
    tr2.stats.channel='tr2'
    tr22 = st.select(station = sta).composite()

    stream = st.select(station = sta) + tr1
    stream += tr2
    stream.plot()
    '''
    #stream.plot(outfile='phil.png')
    '''

if __name__ == '__main__':

    main()
