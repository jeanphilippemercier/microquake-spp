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
    logger = app.get_logger(settings.event_db.log_topic,
                            settings.event_db.log_file_name)

    db_uri = settings.event_db.uri
    db_name = settings.event_db.name

    logger.info("connecting to the event database")
    mongo_conn = MongoDBHandler(uri=db_uri, db_name=db_name)
    collection_name = settings.event_db.traces_collection

    # Create Kafka Object
    kafka_brokers = settings.kafka.brokers
    kafka_topic = settings.event_db.kafka_topic
    consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    logger.info('Consuming Streams from Kafka...')
    for message in consumer:
        logger.info('unpacking the data received from Kafka topic <%s>'
                    % settings.magnitude.kafka_consumer_topic)
        t1 = time()
        data = msgpack.unpack(msg_in.value)
        st = read(BytesIO(data[1]))
        cat = read_events(BytesIO(data[0]))
        t2 = time()
        logger.info('done unpacking the data from Kafka topic <%s> in '
                    '%0.3f seconds'
                    % (settings.magnitude.kafka_consumer_topic, t2 - t1))


        # JP: HANEE YOU SHOULD BE ABLE TO INSERT GET IT FROM THERE
        # ALTERNATIVELY YOU COULD COMMENT THE LINE ABOVE AND SIMPLE READ THE
        # DATA DIRECTLY FROM THE FILES I HAVE UPLOADED ON SLACK UNCOMMENTING
        # THE LINES BELOW
        # from microquake.core.event import read_events, Catalog
        # from microquake.core import read
        # cat = read_events('test.xml')
        # cat = Catalog(events=[cat[0]])
        # st = read('2018-11-08T10:21:49.898496Z.mseed')

        # IF YOU MODIFY THE CONFIG.TOML FILE PLEASE MAKE SURE YOU UPDATE THE
        # CONFIG.TOML.EXAMPLE FILE




        # OLD CODE BELOW

        # should use the REST API POST function instead of the DB handler
        # logger.info('received a message')
        # stime = time.time()
        # stream = read(BytesIO(message.value))
        # etime = time.time() - stime
        #
        # msg_size = (sys.getsizeof(stream) / 1024 / 1024)
        # logger.info("==> consumed stream object from kafka in:", "%.2f" % etime,
        #       "Stream Size:", "%.2f" % msg_size, "MB")
        #
        # stime = time.time()
        # traces_list = stream.to_traces_json()
        # etime = time.time() - stime
        # logger.info("==> converted stream object to json in:", "%.2f" % etime)
        #
        # stime = time.time()
        # mongo_conn.insert_many(collection_name, traces_list)
        # etime = time.time() - stime
        # logger.info("==> inserted stream data into MongoDB in:", "%.2f" % etime)
        #
        # logger.info("=========================================================")
