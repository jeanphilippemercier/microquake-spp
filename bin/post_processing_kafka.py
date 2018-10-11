#from spp.utils import logger as log
#logger = log.get_logger("kafka_events_listener", 'kafka_events_listener.log')
from spp.post_processing.liblog import getLogger
logger = getLogger('-t', logfile="zlog")
#logger = getLogger(logfile="zlog")
import logging

from spp.utils.kafka import KafkaHandler
import numpy as np
import os
import struct
import yaml

from spp.post_processing.make_event import make_event

logger.setLevel(logging.INFO)

def main():

    fname = 'post_processing_kafka'

    logger.info("%s: Start it up" % fname)

    cfg_file = os.path.join(os.environ['SPP_CONFIG'], 'data_connector_config.yaml')

    with open(cfg_file, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['data_connector']

    # Create Kafka Object
    kafka_brokers = params['kafka']['brokers']
    kafka_topic = 'interloc' # PAF - needs to be made configurable

    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    s = struct.Struct('d d d d d')
    logger.info("%s: kafka listener started" % fname)

    for message in consumer:
        logger.info("=== new message in post_processing_kafka consumer ==========")
        from_interloc = s.unpack(message.value)
        logger.info("=== msg key:%s" % message.key.decode('utf-8'))
        logger.info("=== msg val:%s" % (from_interloc,))
        (t, x, y, z, intensity) = from_interloc
        make_event( np.array([t,x,y,z,intensity]), insert_event=True)

if __name__ == "__main__":
    main()
