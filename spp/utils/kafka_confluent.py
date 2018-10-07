from kafka import KafkaProducer, KafkaConsumer
import logging
import sys
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer, AvroConsumer


def on_send_error(excp):
    raise excp


class ConfluentKafkaHandler:

    def __init__(self, brokers_list):
        #logger = logging.getLogger('kafka')
        #logger.addHandler(logging.StreamHandler(sys.stdout))
        #logger.setLevel(logging.ERROR)
        self.schema_registry_url = "http://kafka-node-002:8081"

    @staticmethod
    def get_avro_producer(brokers_list, schema_registry_url, key_schema, value_schema):
        avroProducer = AvroProducer({
            'bootstrap.servers': brokers_list,
            'schema.registry.url': schema_registry_url
            }, default_key_schema=key_schema, default_value_schema=value_schema)
        return avroProducer


    @staticmethod
    def get_avro_consumer(brokers_list, schema_registry_url, group_id=None):
        consumer = AvroConsumer({
            'bootstrap.servers': brokers_list,
            'group.id': 'groupid',
            'schema.registry.url': schema_registry_url})
        return consumer