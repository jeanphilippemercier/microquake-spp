from confluent_kafka import Consumer, KafkaError
from spp.utils.application import Application
from logging import getLogger
from microquake.core import read
from obspy.realtime import RtTrace
from io import BytesIO
import faust
from model import seismic_data
from datetime import timedelta

app = faust.App('event_detection', broker='broker:9092',
                value_serializer='raw')

views = app.Table('views', default=list).hopping(1.2, 1,
                                                 expires=timedelta(hours=5),
                                                 key_index=True).relative_to_field(seismic_data.time)

# print(list(views.keys()))
# class seismic_data(faust.Record):
#     site_id: str
#     mseed: bytes

# continuous_data = app.topic('continuous_data', key_type=str, value_type=bytes)
continuous_data = app.topic('continuous_data', value_type=seismic_data)

@app.agent(continuous_data)
async def sta_lta(stream):
    # async for key, item in stream.group_by(seismic_data.channel_code).items():
    async for key, item in stream.group_by(seismic_data.channel_code).items():
        # print(views[key].values())
        print(views)
        # print(item.station_code, item.channel_code, item.time)
        # input()

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


