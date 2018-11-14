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

from microquake.core.util import tools
from microquake.core.util import plotting as qplot
from microquake.io import msgpack
from spp.utils.kafka import KafkaHandler

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

print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
	print("Received Key:", msg_in.key)

	st = msgpack.unpack(msg_in.value)
	print(st[:-1], type(st[-1]))


# ots = np.array([1000, 25000, 52000]) / 6000.

# srcs = np.array([[651600, 4767420, 200],
# 				[651000, 4768020, 400],
# 				[652000, 4767020, 600]])