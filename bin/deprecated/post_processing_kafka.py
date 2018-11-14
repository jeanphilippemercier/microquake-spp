# from spp.utils import logger as log
# logger = log.get_logger("kafka_events_listener", 'kafka_events_listener.log')

from spp.utils import log_handler
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
import numpy as np
import os
import struct
import yaml
from spp.post_processing.make_event import make_event

logger = log_handler.get_logger("Post Processing Kafka", 'post_processing_kafka.log')
# To be used for loading configurations
config = Application()


def main():
    fname = 'post_processing_kafka'

    logger.info("%s: Start it up" % fname)

    # Create Kafka Object
    kafka_brokers = config.DATA_CONNECTOR['kafka']['brokers']
    kafka_topic = 'interloc'  # PAF - needs to be made configurable

    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

    s = struct.Struct('d d d d d')
    logger.info("%s: kafka listener started" % fname)

    for message in consumer:
        logger.info("=== new message in post_processing_kafka consumer ==========")
        from_interloc = s.unpack(message.value)
        logger.info("=== msg key:%s" % message.key.decode('utf-8'))
        logger.info("=== msg val:%s" % (from_interloc,))
        (t, x, y, z, intensity) = from_interloc
        make_event(np.array([t, x, y, z, intensity]), insert_event=True)


if __name__ == "__main__":
    main()
