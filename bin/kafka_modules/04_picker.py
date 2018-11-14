from importlib import reload
import numpy as np
import matplotlib.pyplot as plt

# from xseis2 import xutil
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
htt = app.get_ttable_h5()

params = app.settings.picker
topic_in = params.kafka_consumer_topic
topic_out = params.kafka_producer_topic
brokers = app.settings.kafka.brokers
kaf_handle = KafkaHandler(brokers)
consumer = KafkaHandler.consume_from_topic(topic_in, brokers, group_id=None)


print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
	print("Received Key:", msg_in.key)

	# msg_in.value = [ot_epoch, lmax[0], lmax[1], lmax[2], vmax, st])
	dat = msgpack.unpack(msg_in.value)
	ot_epoch, loc, vmax, st = dat[0], dat[1:4], dat[4], dat[5]
	# st.filter('bandpass', freqmin=50, freqmax=300)
	stcomp = st.composite()
	# keys = stcomp.unique_stations()

	ista = htt.index_sta(stcomp.unique_stations())
	ix_grid = htt.xyz_to_icol(loc)

	ttP = htt.hf['ttp'][ista, ix_grid]
	ttS = htt.hf['tts'][ista, ix_grid]

	ot_utc = UTCDateTime(ot_epoch)
	ptimes_p = np.array([ot_utc + tt for tt in ttP])
	ptimes_s = np.array([ot_utc + tt for tt in ttS])
	picks = tools.make_picks(stcomp, ptimes_p, 'P', params)
	picks += tools.make_picks(stcomp, ptimes_s, 'S', params)
	# snrs = np.array([pick.snr for pick in picks])
	# qplot.stream(stcomp, picks=picks, color='black', alpha=0.6)

	arrivals = [Arrival(phase=p.phase_hint, pick_id=p.resource_id) for p in picks]

	og = Origin(time=ot_utc, x=loc[0], y=loc[1], z=loc[2], arrivals=arrivals)
	event = Event(origins=[og], picks=picks)
	event.preferred_origin_id = og.resource_id

	# key_out = msg_in.key
	# msg_out = msgpack.pack([event, st])
	# kafka_handler_obj = KafkaHandler(brokers)
	# kafka_handler_obj.send_to_kafka(topic_out, key_out, msg_out)
	# kafka_handler_obj.producer.flush()
