#!/usr/bin/env python3

from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.io import msgpack
from microquake.io.msgpack import pack, unpack
from microquake.core import read
from microquake.core.event import read_events
from io import BytesIO
from time import time
from microquake.waveform.pick import snr_picker

import numpy as np
from microquake.core.event import (Origin, CreationInfo, Event)
from microquake.core import UTCDateTime

if __name__ == '__main__':

    # reading application data
    app = Application()
    settings = app.settings
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)


    kafka_brokers = settings.kafka.brokers
    kafka_topic = settings.picker.kafka_consumer_topic
    kafka_producer_topic = settings.picker.kafka_producer_topic
    kafka_handler = KafkaHandler(kafka_brokers)
    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)
    logger.info('awaiting Kafka mseed messsages')

    # htt = app.get_ttable_h5()

    for message in consumer:
        # TODO need to change the way the incoming message is handled. What
        # is below is for testing purpose
        data = msgpack.unpack(message.value)
        # ot_epoch_or_event, loc, vmax, st = dat[0], dat[1:4], dat[4], dat[5]
        st = read(BytesIO(data[1]))
        cat = read_events(BytesIO(data[0]))

        loc = cat[0].preferred_origin().loc
        logger.info('calculating origin time')
        t0 = time()
        ot_utc = app.estimate_origin_time(st, loc)
        t1 = time()
        logger.info('done calculating origin time in %0.3f seconds' % (t1 - t0))

        st_comp = st.composite()

        logger.info('predicting picks')
        t2 = time()
        o_loc = cat[0].preferred_origin().loc
        o_time = cat[0].preferred_origin().time
        picks = app.synthetic_arrival_times(o_loc, ot_utc)

        t3 = time()
        logger.info('done predicting picks in %0.3f seconds' % (t3 - t2))

        freq_min = params.waveform_filter.frequency_min
        freq_max = params.waveform_filter.frequency_max

        st_raw = st.copy()

        st = st.detrend('demean')
        st = st.taper(max_percentage=0.1, max_length=0.01)
        st = st.filter('bandpass', freqmin=freq_min, freqmax=freq_max)

        st_comp = st.composite()

        logger.info('picking P-waves')
        t4 = time()
        search_window = np.arange(params.p_wave.search_window.start,
                                  params.p_wave.search_window.end,
                                  params.p_wave.search_window.resolution)

        snr_window = (params.p_wave.snr_window.noise,
                      params.p_wave.snr_window.signal)

        snrs_p, p_snr_picks = snr_picker(st, picks,
                                         snr_dt=search_window,
                                         snr_window=snr_window,  filter='P')
        t5 = time()
        logger.info('done picking P-wave in %0.3f seconds' % (t5 - t4))

        logger.info('picking S-waves')
        t6 = time()

        search_window = np.arange(params.s_wave.search_window.start,
                                  params.s_wave.search_window.end,
                                  params.s_wave.search_window.resolution)

        snr_window = (params.s_wave.snr_window.noise,
                      params.s_wave.snr_window.signal)

        snrs_s, s_snr_picks = snr_picker(st, picks,
                                       snr_dt=search_window,
                                       snr_window=snr_window, filter='S')
        t7 = time()
        logger.info('done picking S-wave in %0.3f seconds' % (t7 - t6))

        snr_picks = p_snr_picks + s_snr_picks
        snrs = snrs_p + snrs_s

        snr_picks_filtered = [snr_pick for (snr_pick, snr)
                              in zip(snr_picks, snrs)
                              if snr > params.snr_threshold]

        logger.info('creating arrivals')
        t8 = time()
        arrivals = app.create_arrivals_from_picks(snr_picks_filtered, loc,
                                                  ot_utc)
        t9 = time()
        logger.info('done creating arrivals in %0.3f seconds' % (t9 - t8))

        logger.info('creating new event or appending to existing event')
        t10 = time()

        t11 = time()

        logger.info('Origin time: %s' % ot_utc)
        logger.info('Total number of picks: %d' %
                    len(cat[0].preferred_origin().arrivals))

        logger.info('done creating new event or appending to existing event '
                    'in %0.3f seconds' % (t11 - t10))


        # TODO: Need to check the event database for IMS event.
        # if an event exist, 1) get the event, 2) create a new origin with
        # information from interloc, 3) append picks to event, and 4) append
        # arrivals to the new origin

        origin = Origin()
        origin.time = ot_utc
        origin.x = o_loc[0]
        origin.y = o_loc[1]
        origin.z = o_loc[2]
        origin.arrivals = arrivals
        origin.evaluation_mode = 'automatic'
        origin.evaluation_status = 'preliminary'
        origin.creation_info = CreationInfo(creation_time=UTCDateTime.now())

        cat[0].picks += snr_picks_filtered
        cat[0].origins += [origin]
        cat[0].preferred_origin_id = origin.resource_id.id

        logger.info('sending data to Kafka topic %s' % kafka_producer_topic)
        t12 = time()
        timestamp_ms = int(cat[0].preferred_origin().time.timestamp * 1e3)
        key = str(cat[0].preferred_origin().time).encode('utf-8')
        ev_io = BytesIO()
        cat.write(ev_io, format='QUAKEML')

        # st_io = BytesIO()
        # st.write(st, format='MSEED')

        data_out = pack([ev_io.getvalue(), data[1]])
        kafka_handler.send_to_kafka(kafka_producer_topic,
                                    key,
                                    message=data_out,
                                    timestamp_ms=timestamp_ms)
        t13 = time()
        logger.info('done sending data to Kafka in %0.3f seconds' % (t13 - t12))
        logger.info('completed automatic picking in %0.3f seconds' % (t13 - t0))

        logger.info('=========================================================')