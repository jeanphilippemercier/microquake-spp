from io import BytesIO
from spp.utils.config import Configuration
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from microquake.db.mongo.mongo import MongoDBHandler
import time
import sys

if __name__ == "__main__":


    config = Configuration()

    print("connecting to DB")
    mongo_conn = MongoDBHandler(uri=config.DB_CONFIG['uri'], db_name=config.DB_CONFIG['db_name'])
    collection_name = config.DB_CONFIG['traces_collection']

    # Create Kafka Object
    kafka_brokers = config.IMS_CONFIG['kafka']['brokers']
    kafka_topic = config.IMS_CONFIG['kafka']['topic']
    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

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
        mongo_conn.insert_many(collection_name,traces_list)
        etime = time.time() - stime
        print("==> inserted stream data into MongoDB in:", "%.2f" % etime)

        print("==================================================================")