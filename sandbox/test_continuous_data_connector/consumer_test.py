from confluent_kafka import Consumer, KafkaError
from spp.utils.application import Application
from logging import getLogger
from microquake.core import read
from obspy.realtime import RtTrace
from io import BytesIO
import faust

app = faust.App('event_detection', broker='broker:9092',
                value_serializer='raw',
                topic_partition=4)

continuous_data = app.topic('continuous_data', key_type=str, value_type=bytes)

@app.agent(continuous_data)
async def sta_lta(mseed_chunks):
    async for mseed_chunk in mseed_chunks.items:
        print(site)

# rt_trace = RtTrace(max_length=2)
# # rt_trace.trigger('recstalta', nsta=100, nlta=1000)
#
# # logger = getLogger()
#
# # app = Application()
#
#
# def consumer_msg_iter(consumer, consumer_topic='continuous_data', timeout=0):
#     print("awaiting message on topic %s" % consumer_topic)
#     ct = 0
#     try:
#         while True:
#             msg = consumer.poll(timeout)
#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     print("Reached end of queue!: %s", msg.error())
#                 else:
#                     print("consumer error: %s", msg.error())
#                 continue
#             print("message received on topic %s" % consumer_topic)
#             st = read(BytesIO(msg.value()), format='MSEED').composite()
#             rt_trace.append(st[0])
#             # rt_trace.trigger('recstalta', sta=0.01, lta=0.1)
#             if ct == 100:
#                 return rt_trace
#             ct += 1
#             # from pdb import set_trace; set_trace()
#
#
#     except KeyboardInterrupt:
#         print("received keyboard interrupt")
#
#
# kconsumer =  Consumer(
#             {
#                 "bootstrap.servers": 'broker:9092',
#                 "group.id": app.settings.get('kafka').group_id,
#                 "auto.offset.reset": "earliest",
#             })
#
# consumer_topic = 'continuous_data'
# kconsumer.subscribe([consumer_topic])
#
# rt_trace = consumer_msg_iter(kconsumer, consumer_topic=consumer_topic)


