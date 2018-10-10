from spp.post_processing.liblog import getLogger
#logger = getLogger('-t', logfile="z.log")
logger = getLogger(logfile="zlog")
import logging

import numpy as np
from datetime import datetime
from glob import glob

from microquake.core import read_events
from spp.post_processing.make_event import make_event

def main():

    logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)

    # Small event
    # time = UTCDateTime( datetime(2018, 5, 23, 10, 51, 3, 765333) )
    x = 651280
    y = 4767400
    z = -200
    timestamp = 1530882206.4
    timestamp = 1527072663.765333

    logger.debug("main: this is a debug msg")
    logger.info("main: this is an info msg")
    logger.error("main: this is an error msg")

#2018-07-06T11:21:01.243255Z
    timestamp = 1530876061.2432551
    x = 651185
    y = 4767427
    z = -148

    run_from_xml = True
    data_dir = '/Users/mth/mth/Data/OT_data/'
    if run_from_xml:
        event_files = glob(data_dir + "20180706112101.xml")

        for xmlfile in event_files:
            event = read_events(xmlfile, format='QUAKEML')[0]
            origin = event.origins[0]
            inputs = np.append(origin.loc, origin.time.timestamp)
            print(inputs)
    else:
        inputs = np.array([x,y,z,timestamp])

    make_event( inputs, plot_profiles=False, insert_event=True )
    #make_event( inputs, plot_profiles=False, insert_event=False )
    #make_event( np.array([x,y,z,timestamp]), plot_profiles=True, insert_event=False )

if __name__ == "__main__":
    main()
