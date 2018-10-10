from spp.utils.config import Configuration
from spp.utils.kafka_confluent import ConfluentKafkaHandler
from microquake.core.trace import Trace
import time
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import KafkaError


if __name__ == "__main__":

    config = Configuration()

    # Create Kafka Object
    kafka_brokers = config.IMS_CONFIG['kafka']['brokers']
    kafka_topic = "station_7"
    avro_consumer = ConfluentKafkaHandler.get_avro_consumer(kafka_brokers, "http://kafka-node-002:8081")

    avro_consumer.subscribe([kafka_topic])


    print("Consuming Avro Traces from Kafka...")
    while True:

        try:
            message = avro_consumer.poll(120)

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(message, e))
            break

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(message.error())
                break

        print("Key:", message.key())
        stime = time.time()
        trace_json = message.value()
        etime = time.time() - stime
        print("==> consumed trace object from kafka in:", "%.2f" % etime)

        print(trace_json)

        stime = time.time()
        trace = Trace.create_from_json(trace_json)
        etime = time.time() - stime
        print("==> converted json to trace in:", "%.2f" % etime)

        print(trace)

        print("==================================================================")
