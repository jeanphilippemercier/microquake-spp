from spp.travel_time import core
from spp.utils import get_stations, get_data_connector_parameters
from datetime import datetime
from microquake.core import ctl
from microquake.core import read, UTCDateTime
from microquake.core.stream import Stream
from microquake.core.event import Arrival, Event, Origin
from microquake.core.event import ResourceIdentifier
from obspy.core.event import Event as obsEvent
from microquake.waveform.pick import SNR_picker, calculate_snr, kurtosis_picker
import numpy as np
import os

from importlib import reload
reload(core)

from web_client import get_stream_from_mongo

#from microquake.realtime.signal import kurtosis
from microquake.core import read_events as micro_read_events
from obspy.core.event import read_events
from microquake import nlloc

from helpers import *

import logging
logger = logging.getLogger()
#logger.setLevel(logging.WARNING)
#print(logger.level)
#import matplotlib.pyplot as plt

config_dir = os.environ['SPP_CONFIG']

from spp.utils.kafka import KafkaHandler
from io import BytesIO
def main():
	#event_read = read_events('event4.xml', format='QUAKEML')[0]
	st = read('event_context.mseed', format="MSEED")
	for tr in st:
		print(tr.get_id())
		print(tr.stats)
	
if __name__ == "__main__":
    main()
