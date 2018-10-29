from spp.utils.config import Configuration
import numpy as np
from datetime import datetime
from glob import glob
from microquake.core import read_events
from spp.post_processing.make_event import make_event


#from spp.utils import log_handler
#logger = log_handler.get_logger("Post Processing", 'post_processing.log')
# To be used for loading configurations
config = Configuration()


def main():

    DATA_DIR = '/Users/mth/mth/Data/OT_data/'    # Move to env ?
    DATA_DIR = '/Users/mth/mth/Data/OT_data_event_new/'    # Move to env ?

    intensity = -2.0

    # Small event
    # time = UTCDateTime( datetime(2018, 5, 23, 10, 51, 3, 765333) )
    x = 651280
    y = 4767400
    z = -200
    timestamp = 1530882206.4
    timestamp = 1527072663.765333

    #2018-07-06T11:21:01.243255Z
    timestamp = 1530876061.2432551
    x = 651185
    y = 4767427
    z = -148

# Put it around 10:50:00 on 5/23/18
    timestamp = 1527072600.000

    run_from_xml = False
    run_from_xml = True
    if run_from_xml:
        #event_files = glob(DATA_DIR + "20180706112101.xml")
        #event_files = glob(DATA_DIR + "20180523105102.xml")
        #event_files = glob(DATA_DIR + "20180522053217.xml")
        event_files = glob(DATA_DIR + "2018-09-24T10_55_49.759936Z.xml")

        for xmlfile in event_files:
            event = read_events(xmlfile, format='QUAKEML')[0]
            origin = event.origins[0]
            inputs = np.array([origin.time.timestamp, *origin.loc.tolist(), intensity])
    else:
        inputs = np.array([timestamp, x,y,z, intensity])

    #make_event( inputs, plot_profiles=False, insert_event=True )
    make_event( inputs, plot_profiles=False, insert_event=False )

if __name__ == "__main__":
    main()
