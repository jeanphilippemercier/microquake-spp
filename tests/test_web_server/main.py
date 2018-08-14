import numpy as np
from datetime import datetime

from liblog import getLogger
import logging
logger = getLogger()
logger.setLevel(logging.CRITICAL)
#from microquake.core import UTCDateTime
#logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)

from make_event import make_event
from helpers import get_log_level

def main():
    '''
    intensity = 1.0
    # Big event
    x = 651298
    y = 4767394
    z = -148
    timestamp = 1527072662.2131672
    '''
    logger.setLevel(logging.DEBUG)
    logger.info('main: Test of info')

    # Small event
    # time = UTCDateTime( datetime(2018, 5, 23, 10, 51, 3, 765333) )
    x = 651280
    y = 4767400
    z = -200
    timestamp = 1527072663.765333
    timestamp = 1530882206.4

    '''
    x=651275.000000
    y=4767395.000000
    z=-175.000000
    timestamp = 1527072662.2110002041
    '''
    make_event( np.array([x,y,z,timestamp]), plot_profiles=False, insert_event=True )
    print('main: LOG LEVEL=%s' % (get_log_level(logger.getEffectiveLevel())))

if __name__ == "__main__":
    main()
