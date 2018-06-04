from kafka import KafkaProducer, KafkaConsumer


class KafkaHandler:

    def __init__(self, brokers_list):
        self.producer = KafkaProducer(bootstrap_servers=brokers_list, max_request_size=20971520)

    def send_to_kafka(self, topic_name, message, key=None):
        if key is None:
            self.producer.send(topic=topic_name, value=message)
        else:
            self.producer.send(topic=topic_name, key=key, value=message)

    @staticmethod
    def consume_from_topic(topic_name, brokers_list):
        return KafkaConsumer(topic_name,
                                 #group_id='my-group',
                                 bootstrap_servers=brokers_list)