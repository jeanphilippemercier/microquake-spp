#!/usr/bin/env python3

from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.io import msgpack
from microquake.nlloc import NLL, calculate_uncertainty
from microquake.io.msgpack import pack, unpack
from microquake.core import read
from microquake.core.event import read_events
from io import BytesIO
from time import time
from microquake.core.util import tools
from microquake.waveform.pick import snr_picker

import numpy as np
from scipy.interpolate import interp1d
from microquake.core.stream import Trace
from microquake.core.event import (Catalog, Event, Origin, Pick, Arrival,
                                       WaveformStreamID)
from obspy.realtime.signal import kurtosis
from microquake.core import UTCDateTime


def calculate_origin_time(stream, event_location, app):

    start_times = []
    end_times = []
    sampling_rates = []
    for trace in stream:
        start_times.append(trace.stats.starttime.datetime)
        end_times.append(trace.stats.endtime.datetime)
        sampling_rates.append(trace.stats.sampling_rate)

    min_starttime = UTCDateTime(np.min(start_times)) - 1.0
    max_endtime = UTCDateTime(np.max(end_times))
    max_sampling_rate = np.max(sampling_rates)

    stations = np.unique([tr.stats.station for tr in stream])

    shifted_traces = []
    npts = np.int((max_endtime - min_starttime) * max_sampling_rate)
    t_i = np.arange(0, npts) / max_sampling_rate

    for phase in ['P', 'S']:
        for trace in stream.composite():
            station = trace.stats.station
            tt = app.get_travel_time_grid_point(station, phase, event_location)
            trace.stats.starttime = trace.stats.starttime - tt
            data = trace.data
            data /= np.max(np.abs(data))
            sr = trace.stats.sampling_rate
            startsamp = int((trace.stats.starttime - min_starttime) *
                        trace.stats.sampling_rate)
            endsamp = startsamp + trace.stats.npts
            t = np.arange(startsamp, endsamp) / sr
            try:
                f = interp1d(t, data, bounds_error=False, fill_value=0)
            except:
                continue


            shifted_traces.append(f(t_i))

    shifted_traces = np.array(shifted_traces)
    w_len_sec = 50e-3
    w_len_samp = int(w_len_sec * max_sampling_rate)

    stacked_trace = np.sum(np.array(shifted_traces) ** 2, axis=0)
    stacked_trace /= np.max(np.abs(stacked_trace))
    #
    i_max = np.argmax(np.sum(np.array(shifted_traces) ** 2, axis=0))

    if i_max - w_len_samp < 0:
        pass
        # return

    stacked_tr = Trace()
    stacked_tr.data = stacked_trace
    stacked_tr.stats.starttime = min_starttime
    stacked_tr.stats.sampling_rate = max_sampling_rate

    k = kurtosis(stacked_tr, win=30e-3)
    diff_k = np.diff(k)

    o_i = np.argmax(np.abs(diff_k[i_max - w_len_samp: i_max + w_len_samp])) + \
          i_max - w_len_samp

    origin_time = min_starttime + o_i / max_sampling_rate
    return origin_time


if __name__ == '__main__':

    # reading application data
    app = Application()
    settings = app.settings
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)


    kafka_brokers = settings.kafka.brokers
    kafka_topic = settings.nlloc.kafka_consumer_topic
    kafka_producer_topic = settings.picker.kafka_producer_topic
    kafka_handler = KafkaHandler(kafka_brokers)
    consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)
    logger.info('awaiting Kafka mseed messsages')

    htt = app.get_ttable_h5()

    for message in consumer:
        data = msgpack.unpack(message.value)
        # ot_epoch_or_event, loc, vmax, st = dat[0], dat[1:4], dat[4], dat[5]
        st = read(BytesIO(data[1]))
        cat = read_events(BytesIO(data[0]))
        loc = cat[0].preferred_origin().loc
        logger.info('calculating origin time')
        t0 = time()
        ot_utc = calculate_origin_time(st, loc, app)
        t1 = time()
        logger.info('done calculating origin time in %0.3f' % (t1 - t0))

        st_comp = st.composite()

        logger.info('predicting picks')
        t2 = time()
        picks = []
        for phase in ['P', 'S']:
            for tr in st_comp:
                station = tr.stats.station
                at = ot_utc + app.get_travel_time_grid_point(station, phase,
                                                             loc)

                wf_id = WaveformStreamID(network_code=settings.project_code,
                                         station_code=station)
                pk = Pick(time=at, method='predicted', phase_hint=phase,
                          evaluation_mode='automatic',
                          evaluation_status='preliminary', waveform_id=wf_id)

                picks.append(pk)

        # SNR_dt = np.arange(params.)
        t3 = time()
        logger.info('done predicting picks in %0.3f' % (t3 - t2))

        # snr_picker(st_comp, picks, SNR_dt=None, SNR_window=(1e-3, 20e-3),
        #            filter=None)

        logger.info('picking P-waves')
        t4 = time()
        p_snr_picks = snr_picker(st, picks, SNR_dt=np.linspace(-.08, .05, 200),
                                 SNR_window=(10e-3, 5e-3),  filter='P')
        t5 = time()
        logger.info('done picking P-wave in %0.3f' % (t5 - t4))

        logger.info('picking S-waves')
        t6 = time()
        s_snr_picks = snr_picker(st, picks, SNR_dt=np.linspace(-.05, .05, 200),
                                 SNR_window=(20e-3, 10e-3), filter='S')
        t7 = time()
        logger.info('done picking P-wave in %0.3f' % (t7 - t6))

        snr_picks = p_snr_picks + s_snr_picks


        # st_ids = ['%03d' % int(st_id) for st_id in st_comp.unique_stations()]
        # ista = htt.index_sta(st_ids)
        # ix_grid = htt.xyz_to_icol(loc)
        #
        # ttP = htt.hf['ttp'][ista, ix_grid]
        # ttS = htt.hf['tts'][ista, ix_grid]
        #
        # ptimes_p = np.array([ot_utc + tt for tt in ttP])
        # ptimes_s = np.array([ot_utc + tt for tt in ttS])
        # logger.info('picking P-wave')
        # t2 = time()
        # picks = tools.make_picks(st_comp, ptimes_p, 'P', params)
        # t3 = time()
        # logger.info('done picking P-wave in %0.3f' % (t3 - t2))
        #
        # logger.info('picking S-wave')
        # t4 = time()
        # picks += tools.make_picks(st_comp, ptimes_s, 'S', params)
        # t5 = time()
        # logger.info('done picking P-wave in %0.3f' % (t5 - t4))
        #
        # arrivals = [Arrival(phase=p.phase_hint, pick_id=p.resource_id) for p in picks]
        #
        # og = Origin(time=ot_utc, x=loc[0], y=loc[1], z=loc[2], arrivals=arrivals)
        # event = Event(origins=[og], picks=picks)
        # event.preferred_origin_id = og.resource_id


        # logger.info('sending the data Kafka topic <%s>'
        #             % app.settings.nlloc.kafka_producer_topic)
        # kafka_handler.send_to_kafka(kafka_producer_topic,
        #                             key,
        #                             message=data_out,
        #                             timestamp_ms=timestamp_ms)
        # t5 = time()
        # logger.info('done sending the data to Kafka topic <%s> in %0.3f '
        #             'seconds' % (app.settings.nlloc.kafka_producer_topic,
        #                          t5 - t4))
        logger.info('=========================================================')


    # origin = Origin()
    # origin.x = event_location[0]
    # origin.y = event_location[1]
    # origin.z = event_location[2]
    #
    # origin.time = origin_time
    # origin.evaluation_mode = 'automatic'
    # origin.evaluation_status = 'preliminary'
    # origin.method = 'spp.travel_time.core.create_event'
    #
    # picks = []
    # for phase in ['p', 's']:
    #     for station in stations:
    #         pk = Pick()
    #         tt = get_travel_time_grid_point(station, event_location,
    #                                         phase=phase)
    #         pk.phase_hint = phase.upper()
    #         pk.time = origin_time + tt
    #         pk.evaluation_mode = "automatic"
    #         pk.evaluation_status = "preliminary"
    #         pk.method = 'theoretical prediction from get_travel_time_grid, use_eikonal=False'
    #
    #         waveform_id = WaveformStreamID()
    #         waveform_id.channel_code = ""
    #         waveform_id.station_code = station
    #
    #         pk.waveform_id = waveform_id
    #         picks.append(pk)
    #
    # event = Event()
    # event.origins = [origin]
    # event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id,
    #                                                referred_object=origin)
    # event.picks = picks
    # #event.pick_method = 'predicted from get_travel_time_grid'
    #
    # catalog = Catalog()
    # catalog.events = [event]
    #
    # return catalog