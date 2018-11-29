#!/usr/bin/env python3

from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.waveform import mag
from microquake.io import msgpack
from microquake.core import read_events, read
from io import BytesIO
from time import time

if __name__ == "__main__":

    app = Application()
    settings = app.settings
    logger = app.get_logger(settings.magnitude.log_topic,
                            settings.magnitude.log_file_name)
    site = app.get_stations()
    vp_grid, vs_grid = app.get_velocities()

    kafka_brokers = settings.kafka.brokers
    kafka_topic = settings.magnitude.kafka_consumer_topic
    kafka_producer_topic = settings.magnitude.kafka_producer_topic

    kafka_handler = KafkaHandler(kafka_brokers)
    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

    logger.info("Awaiting Kafka mseed messsages")
    for message in consumer:
        logger.info('unpacking the data received from Kafka topic <%s>'
                    % settings.magnitude.kafka_consumer_topic)
        t1 = time()
        data = msgpack.unpack(message.value)
        st = read(BytesIO(data[1]))
        cat = read_events(BytesIO(data[0]))
        t2 = time()
        logger.info('done unpacking the data from Kafka topic <%s> in '
                    '%0.3f seconds'
                    % (settings.magnitude.kafka_consumer_topic, t2 - t1))

        vp = vp_grid.interpolate(cat[0].preferred_origin().loc)[0]
        vs = vs_grid.interpolate(cat[0].preferred_origin().loc)[0]

        logger.info('calculating the moment magnitude')
        t3 = time()
        cat_out = mag.moment_magnitude(st, cat[0], site, vp, vs, ttpath=None,
        only_triaxial=True, density=2700, min_dist=20, win_length=0.02,
                                   len_spectrum=2 ** 14, freq=100)
        t4 = time()
        logger.info('done calculating the moment magnitude in %0.3f' %
                    (t4 - t3))

        logger.info('packing the data')
        timestamp_ms = int(cat_out[0].preferred_origin().time.timestamp * 1e3)
        key = str(cat_out[0].preferred_origin().time).encode('utf-8')

        ev_io = BytesIO()
        cat_out.write(ev_io, format='QUAKEML')
        data_out = msgpack.pack([ev_io.getvalue(), data[1]])
        t4 = time()
        logger.info('done packing the data in %0.3f seconds' % (t4 - t3))

        logger.info('sending the data Kafka topic <%s>'
                    % app.settings.nlloc.kafka_producer_topic)
        kafka_handler.send_to_kafka(kafka_producer_topic,
                                    key,
                                    message=data_out,
                                    timestamp_ms=timestamp_ms)
        kafka_handler.producer.flush()
        t5 = time()
        logger.info('done sending the data to Kafka topic <%s> in %0.3f '
                    'seconds' % (app.settings.nlloc.kafka_producer_topic,
                                 t5 - t4))

        fname = str(cat_out[0].preferred_origin().time) + '.xml'
        cat_out.write(fname, format='QUAKEML')
        st.write(fname.replace('xml', 'mseed'), format='MSEED')
        logger.info('=========================================================')


