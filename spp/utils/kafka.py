from kafka import KafkaProducer, KafkaConsumer
import logging
import sys


def on_send_error(excp):
    raise excp


class KafkaHandler:

    def __init__(self, brokers_list):
        logger = logging.getLogger('kafka')
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.ERROR)
        self.producer = KafkaProducer(bootstrap_servers=brokers_list,
                                      max_request_size=104857600, buffer_memory=104857600, batch_size=20, request_timeout_ms=100000)

    def send_to_kafka(self, topic_name, message, key=None):
        if key is None:
            return self.producer.send(topic=topic_name, value=message).add_errback(on_send_error)
        else:
            return self.producer.send(topic=topic_name, key=key, value=message).add_errback(on_send_error)

    @staticmethod
    def consume_from_topic(topic_name, brokers_list, group_id=None):
        logger = logging.getLogger('kafka')
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.ERROR)
        if group_id:
            return KafkaConsumer(topic_name,
                                 group_id=group_id,
                                 bootstrap_servers=brokers_list)
        else:
            return KafkaConsumer(topic_name,
                             # group_id='my-group',
                             bootstrap_servers=brokers_list)
