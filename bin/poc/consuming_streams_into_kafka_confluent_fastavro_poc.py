from io import BytesIO
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from microquake.db.mongo.mongo import MongoDBHandler
import time
import sys
import json
import pickle
from spp.utils import avro_handler
import os
from spp.utils.kafka_confluent import ConfluentKafkaHandler
from confluent_kafka import avro
from datamountaineer.schemaregistry.client import SchemaRegistryClient
from datamountaineer.schemaregistry.serializers import MessageSerializer, Util




if __name__ == "__main__":


    config = Application()

    # Create Kafka Object
    kafka_brokers = config.DATA_CONNECTOR['kafka']['brokers']
    kafka_topic = config.DATA_CONNECTOR['kafka']['topic']
    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    avro_schema = avro_handler.parse_avro_schema("mseed_avro_schema.avsc")
    fastavro_schema = avro_handler.parse_fastavro_schema("mseed_avro_schema.avsc")



    # should be config
    stations_count = 109
    producers_dict = {}

    # key_schema_str = """
    # {
    #    "name": "key",
    #    "type": "record",
    #    "fields" : [
    #      {
    #        "name" : "tkey",
    #        "type" : "string"
    #      }
    #    ]
    # }
    # """

    key_schema_str = "\"string\""
    key_schema = avro.loads(key_schema_str)

    avro_producer = ConfluentKafkaHandler.get_avro_producer(kafka_brokers,
                                                            "http://kafka-node-002:8081",
                                                            key_schema, avro_schema)


    print("Consuming Streams from Kafka...")
    for message in consumer:
        print("Key:", message.key)
        stime = time.time()
        stream = read(BytesIO(message.value))
        etime = time.time() - stime

        msg_size = (sys.getsizeof(stream) / 1024 / 1024)
        print("==> consumed stream object from kafka in:", "%.2f" % etime, "Stream Size:", "%.2f" % msg_size, "MB")

        stime = time.time()
        traces_list = stream.to_traces_json()
        etime = time.time() - stime
        print("==> converted stream object to json in:", "%.2f" % etime)

        stime = time.time()
        for trace in traces_list:
            #print("Serializing Trace Started")
            ser_time = time.time()
            #print(trace)

            #print("Serializing Trace Finished and took:", "%.2f" % (time.time() - ser_time))

            #key = { "tkey": str(trace['stats']['starttime']) }
            key = str(trace['stats']['starttime'])

            print("Sending Msg with key:%s, for station:%s" % (key,trace['stats']['station']))
            avro_producer.produce(topic="station_%s" % trace['stats']['station'], value=trace, key=key)

        avro_producer.flush()
        etime = time.time() - stime
        print("==> inserted stream data into Kafka Stations Topics in:", "%.2f" % etime)

        print("==================================================================")
