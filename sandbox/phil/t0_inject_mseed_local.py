from importlib import reload
import numpy as np
import os
import glob
from microquake.core import read
from spp.utils.kafka import KafkaHandler

from microquake.io import msgpack
from spp.utils.application import Application


app = Application()
# data_src = os.path.join(app.common_dir, "synthetic", "chunks")
mseeds = np.sort(glob.glob(app.settings.data_mseed.path))

brokers = app.settings.kafka.brokers
topic = app.settings.interloc.kafka_consumer_topic

kaf_handle = KafkaHandler(brokers)
# topic = 'mseed-1sec'

print("Sending kafka mseed messsages")
for fle in mseeds[:5]:
	fsizemb = os.path.getsize(fle) / 1024.**2
	print("%s | %.2f mb" % (os.path.basename(fle), fsizemb))

	st = read(fle)  # could read fle directly into bytes io
	msg_out = msgpack.pack(st)
	key_out = str(st[0].stats.starttime).encode('utf-8')
	kaf_handle.send_to_kafka(topic, msg_out, key_out)
	kaf_handle.producer.flush()
