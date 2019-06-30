from spp.utils.kafka import KafkaHandler
from spp.utils.application import Application
from microquake.io import waveform

#@HANEE YOU CAN USE THIS CODE TO BUILD THE END POINT THAT SENDS THE MSEED
# CHUNK TO @EUGENE DATA TRANSFORMER.

app = Application()
brokers = app.settings.kafka.brokers
kaf_handle = KafkaHandler(brokers)

params = app.settings.mseed_decomposer
# topic_out = params.kafka_producer_topic
topic_out = app.settings.transformer.kafka_consumer_topic

# @HANEE THE LINE BELOW IS NOT RELEVANT ANYMORE BUT PROVIDES CONTEXT ON WHERE
#
#st = read(params.path_mseed)

dchunks = waveform.decompose_mseed(st.write_bytes())

print("Sending kafka mseed messsages")
for tstamp, dat in dchunks.items():
        key_out = str("keyx").encode('utf-8')
        kaf_handle.send_to_kafka(topic_out, key_out, dat, timestamp_ms=tstamp)
        kaf_handle.producer.flush()
        print(key_out, tstamp)
