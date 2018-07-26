from io import BytesIO

from spp.utils.kafka import KafkaHandler
from spp.utils.config import Configuration

if __name__ == "__main__":

    import os
    import yaml
    from microquake.core.util import serializer
    from microquake.core import read

    config = Configuration()

    # Create Kafka Object
    kafka_brokers = config.IMS_CONFIG['kafka']['brokers']
    kafka_topic = config.IMS_CONFIG['kafka']['topic']

    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    print("Start Consuming....")
    for message in consumer:
        print("Key:", message.key)
        #decoded_msg = serializer.decode_base64(message.value)
        stream = read(BytesIO(message.value))
        print(type(stream))
        print(stream)
        print("==================================================================")
