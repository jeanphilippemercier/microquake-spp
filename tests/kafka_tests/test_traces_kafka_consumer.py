from io import BytesIO
from spp.utils.config import Configuration
from spp.utils.kafka import KafkaHandler
from microquake.core.trace import Trace
from microquake.db.mongo.mongo import MongoDBHandler
import time
import sys
import json
import pickle
from spp.utils import avro_handler
import os
import fastavro


if __name__ == "__main__":

    config = Configuration()

    # Create Kafka Object
    kafka_brokers = config.IMS_CONFIG['kafka']['brokers']
    kafka_topic = "station_11"
    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)


    fastavro_schema = avro_handler.parse_fastavro_schema("mseed_avro_schema.avsc")


    print("Consuming Traces from Kafka...")
    for message in consumer:
        print("Key:", message.key)
        stime = time.time()
        trace_json = avro_handler.decode_fastavro(fastavro_schema, message.value)
        etime = time.time() - stime
        print("==> consumed trace object from kafka in:", "%.2f" % etime)

        print(trace_json)

        stime = time.time()
        trace = Trace.create_from_json(trace_json)
        etime = time.time() - stime
        print("==> converted json to trace in:", "%.2f" % etime)

        print(trace)

        print("==================================================================")
