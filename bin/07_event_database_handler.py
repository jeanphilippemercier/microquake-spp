#!/usr/bin/env python3

from io import BytesIO
from microquake.io import msgpack
from time import time
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from spp.utils import seismic_client

app = Application()
settings = app.settings.data_connector
site = app.get_stations()
tz = app.get_time_zone()

logger = app.get_logger(settings.logger_name, settings.log_filename)

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.event_db.kafka_topic
consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

api_base_url = app.settings.seismic_api.base_url

logger.info('listening for new messages on Kafka topic %s' % kafka_topic)
for message in consumer:
    logger.info('received a message from kafka')
    t0 = time()
    logger.info('unpacking message')
    event_id, event_file, mseed_file, cmseed_file = \
        msgpack.unpack(message.value)
    t1 = time()
    logger.info('done unpacking message in %0.3f seconds' %(t1 - t0))

    logger.info('uploading the data to the database')
    data, files = seismic_client.build_request_data_from_bytes(event_id,
                                                               event_file,
                                                               mseed_file,
                                                               cmseed_file)

    seismic_client.post_event_data(api_base_url, data, files)
    t2 = time()
    logger.info('done uploading the event to the database in %0.3f seconds'
                 % (t2 - t1))
    logger.info('=========================================================')
    logger.info('listening for new messages on Kafka topic %s' % kafka_topic)



