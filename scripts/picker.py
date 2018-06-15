import argparse
import os
from spp.utils import get_stations, decode_mseed


parser = argparse.ArgumentParser(
    description='''Provide accurate estimate of origin time and P- and S-waves
    picks from estimate of event location''')

parser.add_argument('location', type=str,
                    help='estimated event location as a string where X, '
                         'Y and Z coordinates are separate by comma ('
                         'e.g, "X,Y,Z")')

parser.add_argument('mseed', type=str,
                    help='serialized mseed file in Avro format')


if __name__ == "__main__":


    data_dir = os.environ['SPP_DATA']
    config_dir = os.environ['SPP_CONFIG']

    site = get_stations()




