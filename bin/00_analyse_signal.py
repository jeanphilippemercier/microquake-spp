from microquake.IMS import web_client
from microquake.core import UTCDateTime
from spp.utils.application import Application
import pytz
import os
from io import BytesIO
from spp.utils import seismic_client
import numpy as np
from time import time
from IPython.core.debugger import Tracer
from msgpack import packb, unpackb
from dateutil.parser import parse
from confluent_kafka import Producer
from kafka import KafkaConsumer as Consumer
from kafka import KafkaProducer as Producer

__module_name__ = 'signal_analysis'

app = Application(module_name=__module_name__)
# app.init_module()

logger = app.get_logger('signal_analysis', 'signal_analysis.log')

# Init signal analyser

site = app.get_stations()
ims_base_url = app.settings.data_connector.path
start_time = UTCDateTime.now() - 60    # 1 minute before current time
tz = app.get_time_zone()

# start_time = start_time.replace(tzinfo=pytz.utc)

tz = app.get_time_zone()

producer = Producer(bootstrap_servers = app.settings.kafka.brokers,
                    value_serializer=packb)

from kafka import KafkaConsumer
consumer = Consumer(app.settings[__module_name__].kafka_consumer_topic,
                    bootstrap_servers=app.settings.kafka.brokers,
                    value_deserializer=unpackb, group_id='1')

for station in site.stations():
    value = {'station_code': station.code, 'start_time': str(start_time)}
    producer.send(app.settings.signal_analysis.kafka_producer_topic, value)

try:
    for msg_in in consumer:
        try:
            sta = msg_in.value[b'station_code'].decode()
            start_time = parse(msg_in.value[b'start_time'].decode())
            start_time = UTCDateTime(start_time)
            end_time = start_time + app.settings.signal_analysis.interval

            ims_base_url = app.settings.data_connector.path
            c_wf = web_client.get_continuous(ims_base_url, start_time,
                                             end_time, [sta], tz)

            c_wf = c_wf.composite()
            amplitude = np.std(c_wf[0].data)
            dt = end_time - start_time
            nsamp = dt * c_wf[0].stats.sampling_rate
            data = np.nan_to_num(c_wf[0].data)
            non_missing_ratio = len(np.nonzero(data != 0)[0]) / nsamp
            logger.info('energy: %0.3f, integrity: %0.2f' % (amplitude * 1e6,
                                                            non_missing_ratio))

        except Exception as e:
            logger.error(e)

        value = {'station_code': sta, 'start_time': str(end_time)}
        producer.send(app.settings.signal_analysis.kafka_producer_topic, value)
        logger.info('awaiting message from Kafka')

except KeyboardInterrupt:
    logger.info('received keyboard interrupt')

finally:
    logger.info('closing Kafka connection')
    consumer.close()
    logger.info('connection to Kafka closed')


