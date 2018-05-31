from spp.travel_time import core
from spp.utils import get_stations
from importlib import reload
from datetime import datetime
from microquake.core import read, UTCDateTime
from microquake.core.stream import Stream
import numpy as np
import os

from microquake.core.stream import Trace, Stream
from microquake.waveform.pick import SNR_picker, calculate_snr
from IPython.core.debugger import Tracer
import matplotlib.pyplot as plt
from microquake.realtime.signal import kurtosis

reload(core)




if __name__ == "__main__":

    location = [651280, 4767430, -140]
    origin_time = datetime(2018, 4, 14, 19, 44, 22, 92372)
    stations = get_stations()

    data_dir = os.environ['SPP_DATA']

    st2 = read(os.path.join(data_dir, '2018-04-15_034422.mseed'),
               format='MSEED')

    core.init_travel_time()

    cat = core.create_event(st2, location)

    origin_time = cat[0].origins[0].time
    picks = cat[0].picks

    plt.clf()
    st = read(os.path.join(data_dir, '2018-04-15_034422.mseed'), format='MSEED')
    for tr in st.composite():
        for pk in picks:
            if pk.waveform_id.station_code == tr.stats.station:
                if pk.phase_hint == "P":
                    pk_p = pk.time
                else:
                    pk_s = pk.time

        starttime = tr.stats.starttime
        rt = np.arange(0, tr.stats.npts) / tr.stats.sampling_rate
        t = np.array([starttime + dt for dt in rt])

        d = (pk_p - origin_time) * 5000

        data = tr.data
        data /= np.max(np.abs(data))

        plt.plot(t, tr.data*20 + d, 'k')
        plt.vlines(pk_p, d-20, d+20, colors='r', linestyles=':')
        plt.vlines(pk_s, d-20, d+20, colors='r', linestyles='--')


    plt.show()



    # starttimes = []
    # endtimes = []
    # sampling_rates = []
    # for trace in st:
    #     starttimes.append(trace.stats.starttime.datetime)
    #     endtimes.append(trace.stats.endtime.datetime)
    #     sampling_rates.append(trace.stats.sampling_rate)
    #
    # min_starttime = UTCDateTime(np.min(starttimes)) - 0.5
    # max_endtime = UTCDateTime(np.max(endtimes))
    # max_sampling_rate = np.max(sampling_rates)
    #
    # nb_pts = np.int((max_endtime - min_starttime) * max_sampling_rate)
    #
    # core.init_travel_time()
    #
    # traces_shifted_p = []
    # traces_shifted_s = []
    #
    # t_i = np.arange(0, nb_pts) / max_sampling_rate
    #
    # for station in stations.stations():
    #     print(station.code)
    #     station_id = station.code
    #     st_station = st.copy().select(station=station_id).composite()
    #     if not st_station:
    #         continue
    #
    #     (st_p, tt) = core.get_arrival_time_station(station_id, 'P', location,
    #                                                origin_time, st_station)
    #
    #     for trace in st_p:
    #         data = trace.data
    #         data = data / np.max(np.abs(data))
    #         sr = trace.stats.sampling_rate
    #         startsamp = int((trace.stats.starttime - min_starttime) *
    #                     trace.stats.sampling_rate)
    #         endsamp = startsamp + trace.stats.npts
    #         t = np.arange(startsamp, endsamp) / sr
    #         f = interp1d(t, data, bounds_error=False, fill_value=0)
    #
    #         traces_shifted_p.append(f(t_i))
    #
    #     st_station = st.copy().select(station=station_id).composite()
    #
    #     st_s = core.get_arrival_time_station(station_id, 'S', location,
    #                                           origin_time, st_station)
    #
    #     for trace in st_s:
    #         data = trace.data
    #         data = data / np.max(np.abs(data))
    #         sr = trace.stats.sampling_rate
    #         startsamp = int((trace.stats.starttime - min_starttime) *
    #                     trace.stats.sampling_rate)
    #         endsamp = startsamp + trace.stats.npts
    #         t = np.arange(startsamp, endsamp) / sr
    #         f = interp1d(t, data, bounds_error=False, fill_value=0)
    #
    #         traces_shifted_s.append(f(t_i))
    #
    #
    # # replacing NaN by 0
    #
    # # traces_shifted_p[traces_shifted_p == np.nan] = 0
    # # traces_shifted_s[traces_shifted_s == np.nan] = 0
    #
    # w_len_sec = 50e-3
    # w_len_samp = int(w_len_sec * max_sampling_rate)
    #
    # traces_shifted = np.vstack((traces_shifted_p, traces_shifted_s))
    # stacked = np.sum(np.array(traces_shifted)**2, axis=0)
    # stacked = stacked / np.max(np.abs(stacked))
    # #
    # i_max = np.argmax(np.sum(np.array(traces_shifted)**2, axis=0))
    #
    # if i_max - w_len_samp < 0:
    #     pass
    #     # return
    #
    # stacked_tr = Trace()
    # stacked_tr.data = stacked
    # stacked_tr.stats.starttime = min_starttime
    # stacked_tr.stats.sampling_rate = max_sampling_rate
    #
    # k = kurtosis(stacked_tr, win=30e-3)
    # diff_k = np.diff(k)
    #
    # o_i = np.argmax(np.abs(diff_k[i_max - w_len_samp: i_max + w_len_samp])) + \
    #       i_max - w_len_samp
    #
    # origin_time = min_starttime + o_i / max_sampling_rate


    #
    # w_len = 512
    # traces_shifted_2 = []
    # for k, t1 in enumerate(traces_shifted_p[3:4]):
    #     for t2 in traces_shifted_p[10:20]:
    #         print('bubu')
    #         d1 = t1
    #         D1 = np.fft.fft(d1)
    #         d2 = t2
    #         D2 = np.fft.fft(d2)
    #
    #         CC = D1 * np.conj(D2)
    #         cc = np.fft.fftshift(np.real(np.fft.ifft(CC)))
    #         l_cc = len(cc)
    #         i = np.argmax(np.abs(cc[l_cc-w_len:l_cc+w_len])) - w_len
    #
    #         d2 = np.roll(d2, i)
    #         traces_shifted_2.append(d2)

            # if i > 0:
            #     # plt.plot(d1[i:])
            #     # plt.plot(d2)
            #     traces_shifted_2.append(np.pad(d2), i, mode='constant')[
            #     0:len(d2)]
            # else:
            #     # plt.plot(d1)
            #     # plt.plot(d2[-i:])
            #     traces_shifted_2.append(d2[-i:])
            #

            # plt.show()
            # input('bubu')




    # st2 = Stream(traces=traces_shifted)

        # trace = trace.trim(starttime=min_starttime, endtime=max_endtime,
        #                    fill_value=0)
        # data = trace.data[:nb_pts]
        # if len(data) != nb_pts:
        #     continue
        #
        # data = data / np.max(np.abs(data))
        # traces_shifted.append(data)

