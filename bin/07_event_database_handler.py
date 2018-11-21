#!/usr/bin/env python3

from io import BytesIO
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.io import msgpack
from time import time
import requests


def construct_file(file_bytes, global_filename=None):
    if global_filename is not None:
        file_bytes.name = global_filename
    file_bytes.seek(0)
    return file_bytes


def construct_all_event_files(event_file_bytes,
                              waveform_file_bytes,
                              context_file_bytes, global_filename):
    data = dict()
    data['event'] = construct_file(event_file_bytes, global_filename + ".xml")
    data['waveform'] = construct_file(waveform_file_bytes, global_filename + ".mseed")
    data['context'] = construct_file(context_file_bytes, global_filename + ".mseed")
    return data


def construct_one_event_file(event_id, file_type, file_bytes, global_filename):
    data = dict()

    data['event_resource_id'] = event_id

    if file_type == 'QUAKEML':
        data['event'] = construct_file(file_bytes, global_filename + ".xml")
    elif file_type == 'MSEED':
        data['waveform'] = construct_file(file_bytes, global_filename + ".mseed")
    elif file_type == 'CONTEXT_MSEED':
        data['context'] = construct_file(file_bytes, global_filename + ".mseed")
    return data


def post_event_files_to_api(full_url, files_data):
    result = requests.post(full_url, files=files_data)
    print(result)


if __name__ == "__main__":

    app = Application()
    settings = app.settings
    logger = app.get_logger(settings.event_db.log_topic,
                            settings.event_db.log_file_name)

    db_uri = settings.event_db.uri
    db_name = settings.event_db.name
    EVENTS_API_URL = settings.seismic_api.events_url

    # I guess will not used here in that module
    # logger.info("connecting to the event database")
    # mongo_conn = MongoDBHandler(uri=db_uri, db_name=db_name)
    # collection_name = settings.event_db.traces_collection

    # Create Kafka Object
    kafka_brokers = settings.kafka.brokers
    kafka_topic = settings.event_db.kafka_topic
    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

    logger.info('Consuming Streams from Kafka...')
    for message in consumer:
        logger.info('unpacking the data received from Kafka topic <%s>'
                    % settings.magnitude.kafka_consumer_topic)
        t1 = time()

        data = msgpack.unpack(message.value)
        # event_id, file_type, data = msgpack.unpack(message.value)

        # These might not be used as we will send the files bytes direct
        # st = read(BytesIO(data[1]))
        # cat = read_events(BytesIO(data[0]))

        # We need to extract filename from event file as example:
        global_filename = "test"
        # Sending the context same as stream file, just for now as temp solution
        files_dict = construct_one_event_file(BytesIO(data[0]), BytesIO(data[1]),
                                              BytesIO(data[2]), global_filename)
        post_event_files_to_api(EVENTS_API_URL, files_dict)

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
