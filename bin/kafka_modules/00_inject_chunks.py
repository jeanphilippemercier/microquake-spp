from importlib import reload
import numpy as np
# import os
import glob
from microquake.core import read
from spp.utils.kafka import KafkaHandler
# from microquake.io import msgpack
from spp.utils.application import Application
from microquake.io import waveform

app = Application()
brokers = app.settings.kafka.brokers
kaf_handle = KafkaHandler(brokers)

params = app.settings.mseed_decomposer
topic_out = app.settings.transformer.kafka_consumer_topic

# mseeds = np.sort(glob.glob(app.settings.data_local.ms_chunks))
mseeds = np.sort(glob.glob(app.settings.data_local.ms_ot))


for i, mseed in enumerate(mseeds[:4]):

	st = read(mseed)
	dchunks = waveform.decompose_mseed(st.write_bytes())

	print("Sending kafka mseed messsages")
	for tstamp, dat in dchunks.items():
	        key_out = str("keyx").encode('utf-8')
	        kaf_handle.send_to_kafka(topic_out, key_out, dat, timestamp_ms=tstamp)
	        kaf_handle.producer.flush()
	        print(key_out, tstamp)
