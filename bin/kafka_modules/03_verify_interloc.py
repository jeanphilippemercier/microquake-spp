from importlib import reload
import numpy as np
import matplotlib.pyplot as plt


from xseis2 import xutil
from microquake.core.event import Arrival, Event, Origin, Pick
from microquake.core.event import ResourceIdentifier
# from microquake.core import Stream
# from microquake.core import Trace
from microquake.core import read
from microquake.core import UTCDateTime
from spp.utils.application import Application

# from microquake.core.util import tools
from microquake.core.util import plotting as qplot
from microquake.io import msgpack
from spp.utils.kafka import KafkaHandler

from microquake.core.util import tools
from microquake.core.event import read_events
import os

plt.ion()

app = Application()
# nthreads = app.settings.interloc.threads
# wlen_sec = app.settings.interloc.wlen_seconds
# dsr = app.settings.interloc.dsr
# debug = app.settings.interloc.debug
params = app.settings.picker
topic_in = params.kafka_consumer_topic
topic_out = params.kafka_producer_topic
brokers = app.settings.kafka.brokers
kaf_handle = KafkaHandler(brokers)
# consumer = KafkaHandler.consume_from_topic(topic_in, brokers, group_id='group')
consumer = KafkaHandler.consume_from_topic(topic_in, brokers, group_id=None)

cat = read_events(app.settings.chunk_injector.path_xml)
epochs = np.array([tools.datetime_to_epoch_sec(ev.origins[0].time.datetime) for ev in cat])

print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
	print("Received Key:", msg_in.key)

	dat = msgpack.unpack(msg_in.value)
	# msg_out = msgpack.pack([ot_epoch, lmax[0], lmax[1], lmax[2], vmax, st])
	ot_epoch, loc, vmax, st = dat[0], dat[1:4], dat[4], dat[5]

	ix = np.argmin(np.abs(epochs - ot_epoch))
	loc_true = cat.events[ix].origins[0].loc
	print("loc, vmax        : ", loc, vmax)
	print("time diff (ms)   : ", ot_epoch - epochs[ix])
	print("location diff (m): ", loc_true - loc)
	print("LOC_MATCH: ", np.allclose(loc_true, loc))
	print("\n")
