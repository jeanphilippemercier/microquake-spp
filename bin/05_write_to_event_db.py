from io import BytesIO
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from microquake.db.mongo.mongo import MongoDBHandler
import time
import sys

if __name__ == "__main__":

    app = Application()
    settings = app.settings
    logger = app.get_logger(settings.magnitude.log_topic,
                            settings.magnitude.log_file_name)

    db_uri = settings.event_db.uri
    db_name = settings.event_db.name

    logger.info("connecting to the event database")
    mongo_conn = MongoDBHandler(uri=db_uri, db_name=db_name)
    collection_name = config.DB['traces_collection']

    # Create Kafka Object
    kafka_brokers = config.DATA_CONNECTOR['kafka']['brokers']
    kafka_topic = config.DATA_CONNECTOR['kafka']['topic']
    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    print("Consuming Streams from Kafka...")
    for message in consumer:
        print("Key:", message.key)
        stime = time.time()
        stream = read(BytesIO(message.value))
        etime = time.time() - stime

        msg_size = (sys.getsizeof(stream) / 1024 / 1024)
        print("==> consumed stream object from kafka in:", "%.2f" % etime,
              "Stream Size:", "%.2f" % msg_size, "MB")

        stime = time.time()
        traces_list = stream.to_traces_json()
        etime = time.time() - stime
        print("==> converted stream object to json in:", "%.2f" % etime)

        stime = time.time()
        mongo_conn.insert_many(collection_name,traces_list)
        etime = time.time() - stime
        print("==> inserted stream data into MongoDB in:", "%.2f" % etime)

        print("==================================================================")
