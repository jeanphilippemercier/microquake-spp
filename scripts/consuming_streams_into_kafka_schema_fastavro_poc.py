from io import BytesIO
from spp.utils.config import Configuration
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from microquake.db.mongo.mongo import MongoDBHandler
import time
import sys
import json
import pickle
from spp.utils import avro_handler
import os
from datamountaineer.schemaregistry.client import SchemaRegistryClient
from datamountaineer.schemaregistry.serializers import MessageSerializer, Util


if __name__ == "__main__":


    config = Configuration()

    # Create Kafka Object
    kafka_brokers = config.IMS_CONFIG['kafka']['brokers']
    kafka_topic = config.IMS_CONFIG['kafka']['topic']
    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    #avro_schema = avro_handler.parse_avro_schema("mseed_avro_schema.avsc")
    #fastavro_schema = avro_handler.parse_fastavro_schema("mseed_avro_schema.avsc")

    # should be config
    stations_count = 109
    producers_dict = {}
    kafka_handler = KafkaHandler(kafka_brokers)

    # Initialize the Schema client
    client = SchemaRegistryClient(url='http://kafka-node-002:8081')
    schema_id, avro_schema, schema_version = client.get_latest_schema('traces-schema')
    # encode a record to put onto kafka
    serializer = MessageSerializer(client)

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
            print("Serializing Trace Started")
            ser_time = time.time()
            #print(trace)

            # use the schema id directly
            msg_encoded = serializer.encode_record_with_schema_id(schema_id, avro_schema, trace)

            print("Serializing Trace Finished and took:", "%.2f" % (time.time() - ser_time))

            kafka_handler.send_to_kafka("station_%s" % trace['stats']['station'], msg_encoded, str(trace['stats']['starttime']).encode('utf-8'))
        kafka_handler.producer.flush() 
        etime = time.time() - stime
        print("==> inserted stream data into Kafka Stations Topics in:", "%.2f" % etime)

        print("==================================================================")
