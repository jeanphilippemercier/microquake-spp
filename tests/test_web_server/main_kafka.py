from spp.utils.kafka import KafkaHandler
import numpy as np
import os
import struct
import yaml

from make_event import make_event

from spp.utils import logger as log


logger = log.get_logger("kafka_events_listener", 'kafka_events_listener.log')


def main():


    config_dir = os.environ['SPP_CONFIG']
    fname = os.path.join(config_dir, 'data_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['data_connector']

    # Create Kafka Object
    kafka_brokers = params['kafka']['brokers']
    kafka_topic = 'interloc' # PAF - needs to be made configurable

    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    '''
    t=1527072662.2110002041
    x=651275.000000
    y=4767395.000000
    z=-175.000000
    '''

    s = struct.Struct('d d d d d')
    logger.info("main_kafka.py: kafka listener started")
    for message in consumer:
        logger.info("=== new message in make_event consumer ==========")
        #logger.info("Key:", message.key.decode('utf-8'))
        from_interloc = s.unpack(message.value)
        #print(from_interloc)
        (t, x, y, z, intensity) = from_interloc
        #(intensity, x, y, z, t) = from_interloc
        #print('got t=',t)
        #print('%15.10f' % t)
        #print('x=%f' % x)
        #print('y=%f' % y)
        #print('z=%f' % z)
        #print("==================================================================")
        logger.info("Call make_event: x=%.1f, y=%.1f, z=%.1f, t=%.1f" % (x,y,z,t))
        make_event( np.array([x,y,z,t]), insert_event=True)
    #exit()

if __name__ == "__main__":
    main()
