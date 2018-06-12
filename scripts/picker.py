import argparse
import os
from spp.utils import get_stations
from io import BytesIO
import numpy as np
from microquake.core import read
from IPython.core.debugger import Tracer
from importlib import reload

from spp.travel_time import init_travel_time, create_event


def predict_travel_time(location_str, mseed_bytes):
    """

    :param location_str: estimated event location as a string where X,
                         Y and Z coordinates are separate by comma (
                         e.g., "X,Y,Z")
    :param mseed_bytes: MSEED binary string
    :return:
    """

    # TODO using grid loaded into memory (Redis?) so to not have to load the
    # grids every time the module is run.

    init_travel_time()

    location = np.array([float(coord) for coord in location_str.split(',')])

    buf = BytesIO(mseed_bytes)
    st = read(buf, format='MSEED')

    cat = create_event(st.copy(), location)

    ##########
    # INSERT PICKER HERE, the picker function should take two non keyword
    # arguments (a stream and a catalogue and as many as keyword arguments as
    # you want. the keyword arguments will be passed from a config file
    # For instance in let say we get the parameter from a yaml file we would get
    # >> import yaml
    # >> params = yaml.load(...)
    # >> cat_out = picker(stream, catalogue, **params[picker][kwargs]
    # the SNR picker currently returns a catalogue but could probably simply
    # return a list of picks.
    # example of how I use the SNR picker below.
    # Note 1: The SNR picker is not optimized and does not currently work well.
    # Note 2: you will probably want to filter the waveforms. I have tried
    # filtering between 50 and 300 Hz. You may want to play more with the filter
    # Note, that the origin time should also be corrected following the
    # picking. Bias which will be the mean of the difference between the
    # initial picks and the new picks (Bias = new picks - initial picks) should
    #  be added to the origin time.

    return cat


if __name__ == "__main__":

    # This function will be launched by a module launcher

    parser = argparse.ArgumentParser(
    description='''Provide accurate estimate of origin time and P- and S-waves
    picks from estimate of event location''')

    parser.add_argument('location', type=str,
                        help='estimated event location as a string where X, '
                         'Y and Z coordinates are separate by comma ('
                         'e.g, "X,Y,Z")')

    parser.add_argument('mseed', type=str, help='MSEED binary string')


    args = parser.parse_args()

    predict_travel_time(args.location, args.mseed)







