from io import BytesIO
from spp.utils.config import Configuration
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from microquake.db.mongo.mongo import MongoDBHandler
import time
import sys
import json
import pickle

if __name__ == "__main__":


    config = Configuration()

    # Create Kafka Object
    kafka_brokers = config.IMS_CONFIG['kafka']['brokers']
    kafka_topic = config.IMS_CONFIG['kafka']['topic']
    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    # should be config
    stations_count = 109
    producers_dict = {}
    kafka_handler = KafkaHandler(kafka_brokers)

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
            print("---- Trace:")
            print(trace)
            msg = json.dumps(trace).encode('utf-8')
            # msg = pickle.dumps(trace)
            # msg = trace
            kafka_handler.send_to_kafka("station_%s" % trace['stats']['station'], msg, str(trace['stats']['starttime']).encode('utf-8'))
        kafka_handler.producer.flush() 
        etime = time.time() - stime
        print("==> inserted stream data into Kafka Stations Topics in:", "%.2f" % etime)

        print("==================================================================")
