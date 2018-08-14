from spp.utils.kafka import KafkaHandler
import struct
import yaml

import logging
logger = logging.getLogger()
#logger.setLevel(logging.WARNING)
#print(logger.level)

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
    for message in consumer:
        print("==================================================================")
        print("Key:", message.key)
        from_interloc = s.unpack(message.value)
        print(from_interloc)
        (t, x, y, z, intensity) = from_interloc
        #(intensity, x, y, z, t) = from_interloc
        print('got t=',t)
        print('%15.10f' % t)
        print('x=%f' % x)
        print('y=%f' % y)
        print('z=%f' % z)
        print("==================================================================")
        make_event( np.array([x,y,z,t]) )
    exit()
