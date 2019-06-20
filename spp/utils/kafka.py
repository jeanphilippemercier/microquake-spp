from kafka import KafkaProducer, KafkaConsumer
import sys


def on_send_error(excp):
    raise excp


class KafkaHandler:

    def __init__(self, brokers_list, batch_size=20, msg_maxsize_mb=500):
        # self.brokers = brokers_list

        maxbytes = int(msg_maxsize_mb * 1024 ** 2)
        self.producer = KafkaProducer(bootstrap_servers=brokers_list,
                                      buffer_memory=maxbytes,
                                      max_request_size=maxbytes,
                                      batch_size=batch_size,
                                      request_timeout_ms=100000)

    def send_to_kafka(self, topic, key, message=None, timestamp_ms=None):

        return self.producer.send(topic=topic, key=key, value=message,
                                  timestamp_ms=timestamp_ms).add_errback(
                                  on_send_error)

    @staticmethod
    def consume_from_topic(topic_name, brokers_list, group_id=None):
        return KafkaConsumer(topic_name,
                             group_id=group_id,
                             bootstrap_servers=brokers_list)
