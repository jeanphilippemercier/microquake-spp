from io import BytesIO

from spp.utils.kafka import KafkaHandler

if __name__ == "__main__":

    import os
    import yaml
    from microquake.core.util import serializer
    from microquake.core import read


    config_dir = os.environ['SPP_CONFIG']
    #common_dir = os.environ['SPP_COMMON']

    fname = os.path.join(config_dir, 'ims_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['ims_connector']

    # Create Kafka Object
    kafka_brokers = params['kafka']['brokers']
    kafka_topic = params['kafka']['topic']

    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    for message in consumer:
        print("Key:", message.key)
        #decoded_msg = serializer.decode_base64(message.value)
        stream = read(BytesIO(message.value))
        print(type(stream))
        print(stream)
        print("==================================================================")
