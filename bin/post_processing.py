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

    DATA_DIR = '/Users/mth/mth/Data/OT_data/'    # Move to env ?

    logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)

    intensity = 1.0

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

    run_from_xml = True
    if run_from_xml:
        event_files = glob(DATA_DIR + "20180706112101.xml")

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
