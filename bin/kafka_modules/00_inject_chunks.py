from importlib import reload
# import numpy as np
# import os
# import glob
from microquake.core import read
from spp.utils.kafka import KafkaHandler
# from io import BytesIO
# from microquake.io import msgpack
from spp.utils.application import Application
# from struct import unpack
# from datetime import datetime
# from microquake.core.util import tools
from microquake.io import waveform

app = Application()
brokers = app.settings.kafka.brokers
kaf_handle = KafkaHandler(brokers)

params = app.settings.chunk_injector
# topic_out = params.kafka_producer_topic
topic_out = app.settings.transformer.kafka_consumer_topic
st = read(params.path_mseed)
dchunks = waveform.decompose_mseed(st.write_bytes())

print("Sending kafka mseed messsages")
for tstamp, dat in dchunks.items():
        key_out = str("keyx").encode('utf-8')
        kaf_handle.send_to_kafka(topic_out, key_out, dat, timestamp_ms=tstamp)
        kaf_handle.producer.flush()
        print(key_out, tstamp)
