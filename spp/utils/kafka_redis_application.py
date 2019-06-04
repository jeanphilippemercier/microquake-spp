import uuid
from time import time

from confluent_kafka import Consumer, KafkaError, Producer
from tenacity import (after_log, before_log, retry, stop_after_attempt,
                      wait_exponential)

from .application import Application
from loguru import logger


class KafkaRedisApplication(Application):
    def __init__(
        self, toml_file=None, module_name=None, processing_flow_name="automatic"
    ):
        super(KafkaRedisApplication, self).__init__(
            toml_file=toml_file,
            module_name=module_name,
            processing_flow_name=processing_flow_name,
        )
        logger.info("setting up Kafka")
        self.producer = self.get_kafka_producer(logger=logger)
        self.consumer = self.get_kafka_consumer(logger=logger)
        self.consumer_topic = self.get_consumer_topic(
            self.processing_flow_steps,
            self.dataset,
            self.__module_name__,
            self.trigger_data_name,
        )
        if self.consumer_topic is not "":
            self.consumer.subscribe([self.consumer_topic])
        logger.info("done setting up Kafka")

        logger.info("init connection to redis")
        self.redis_conn = self.init_redis()
        logger.info("connection to redis database successfully initated")

    def init_redis(self):
        from redis import StrictRedis

        return StrictRedis(**self.settings.get('redis_db'))

    def get_kafka_producer(self, logger=None, **kwargs):
        return Producer(
            {"bootstrap.servers": self.settings.get('kafka').brokers}, logger=logger
        )

    def get_kafka_consumer(self, logger=None, **kwargs):
        return Consumer(
            {
                "bootstrap.servers": self.settings.get('kafka').brokers,
                "group.id": self.settings.get('kafka').group_id,
                "auto.offset.reset": "earliest",
            },
            logger=logger,
        )

    def close(self):
        super(KafkaRedisApplication, self).close()
        logger.info("closing Kafka connection")
        self.consumer.close()
        logger.info("connection to Kafka closed")

    def consumer_msg_iter(self, timeout=0):
        logger.info("awaiting message on topic %s", self.consumer_topic)
        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of queue!: %s", msg.error())
                    else:
                        logger.error("consumer error: %s", msg.error())
                    continue
                logger.info("message received on topic %s", self.consumer_topic)
                redis_key = msg.value()
                yield self.get_redis_msg(redis_key)
                logger.info("awaiting message on topic %s", self.consumer_topic)

        except KeyboardInterrupt:
            logger.info("received keyboard interrupt")

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(7),
    )
    def get_redis_msg(self, redis_key):
        logger.info("getting data from Redis (key: %s)", redis_key)
        t0 = time()
        try:
            redis_data = self.redis_conn.get(redis_key)
        except Exception as e:
            logger.error("Could not retrieve redis data for key = %s", redis_key)
            raise e
        t1 = time()
        logger.info("done getting data from Redis in %0.3f seconds", (t1 - t0))
        return redis_data

    def send_message(self, cat, stream, topic=None):
        msg = super(KafkaRedisApplication, self).send_message(cat, stream, topic)

        redis_key = str(uuid.uuid4())
        logger.info("sending data to Redis with redis key = %s", redis_key)
        self.redis_conn.set(redis_key, msg, ex=self.settings.get('redis_extra').ttl)
        logger.info("done sending data to Redis")

        if topic is None:
            topic = self.get_producer_topic(self.dataset, self.__module_name__)
        logger.info("sending message to kafka on topic %s", topic)
        self.producer.produce(topic, redis_key)
        logger.info("done sending message to kafka on topic %s", topic)

    def receive_message(self, msg_in, callback, **kwargs):
        """
        receive message
        :param callback: callback function signature must be as follows:
        def callback(cat=None, stream=None, extra_msg=None, logger=None,
        **kwargs)
        :param msg_in: message read from kafka
        :return: what callback function returns
        """
        return super(KafkaRedisApplication, self).receive_message(
            msg_in, callback, **kwargs
        )
