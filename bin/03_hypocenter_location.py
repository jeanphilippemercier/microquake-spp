#!/usr/bin/env python3

from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.nlloc import NLL
from microquake.io.msgpack import pack, unpack
from microquake.core import read
from microquake.core.event import read_events
from io import BytesIO
from time import time

if __name__ == '__main__':

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger(settings.nlloc.log_topic,
                            settings.nlloc.log_file_name)

    # TODO NEED TO CHECK IF VELOCITY MODEL IS CURRENT. THIS WILL BE DONE
    # THROUGH A CALL TO THE API. IF THE VELOCITY IS NOT CONSTANT, VELOCITY
    # MODEL WILL NEED TO BE DOWNLOADED AND TRAVEL TIME RECALCULATED

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()
    params = app.settings.nlloc

    # Preparing NonLinLoc
    logger.info('preparing NonLinLoc')
    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
              sensors=sensors, params=params)

    kafka_brokers = settings.kafka.brokers
    kafka_topic = settings.nlloc.kafka_consumer_topic
    kafka_producer_topic = settings.nlloc.kafka_producer_topic
    kafka_handler = KafkaHandler(kafka_brokers)
    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)
    logger.info('awaiting Kafka mseed messsages')
    for message in consumer:
        # logger.info("Received message with key:", msg_in.key)
        logger.info('unpacking the data received from Kafka topic <%s>'
                    % settings.nlloc.kafka_consumer_topic)
        t1 = time()
        data = unpack(message.value)
        st = read(BytesIO(data[1]))
        cat = read_events(BytesIO(data[0]))
        t2 = time()
        logger.info('done unpacking the data from Kafka topic <%s> in '
                    '%0.3f seconds'
                    % (settings.nlloc.kafka_consumer_topic, t2 - t1))

        logger.info('done NonLinLoc')
        cat_out = nll.run_event(cat[0].copy())
        t3 = time()
        logger.info('done running NonLinLoc in %0.3f seconds' % (t3 - t2))

        logger.info('packing the data')
        timestamp_ms = int(cat_out[0].preferred_origin().time.timestamp * 1e3)
        key = str(cat_out[0].preferred_origin().time).encode('utf-8')

        ev_io = BytesIO()
        cat_out.write(ev_io, format='QUAKEML')
        data_out = pack([ev_id, ev_io.getvalue(), data[1]])
        t4 = time()
        logger.info('done packing the data in %0.3f seconds' % (t4 - t3))

        logger.info('sending the data Kafka topic <%s>'
                    % app.settings.nlloc.kafka_producer_topic)
        kafka_handler.send_to_kafka(kafka_producer_topic,
                                    key,
                                    message=data_out,
                                    timestamp_ms=timestamp_ms)
        t5 = time()
        logger.info('done sending the data to Kafka topic <%s> in %0.3f '
                    'seconds' % (app.settings.nlloc.kafka_producer_topic,
                                 t5 - t4))
        logger.info('=========================================================')




