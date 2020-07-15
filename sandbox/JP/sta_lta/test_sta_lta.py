from microquake.core import read

from obspy.signal.trigger import recursive_sta_lta, trigger_onset
from obspy.signal.trigger import coincidence_trigger

import matplotlib.pyplot as plt

st = read('2020-05-30T02_21_50.000000Z.mseed')
st = st.detrend('demean').detrend('linear')
st = st.filter('highpass', freq=15)

trgs = coincidence_trigger('recstalta', 5, 4, st, 5, max_trigger_length=3,
delete_long_trigger=True, details=True, sta=10e-3, lta=100e-3)

# sr_max =
# for tr in st:
#     if tr.stats.sampling_rate > sr_max:
#         sr_max = tr.stats.sampling_rate
#
# st_interp = st.copy().interpolate(sampling_rate = sr_max)


# for tr in st:
#     cft = recursive_sta_lta(tr.data, int(10e-3 * df), int(100e-3 * df))


sensors = []
on = []
off = []

for tr in st:

    # st_146 = st.select(station='146').copy()[0]
    # cft = st_146.copy().trigger('recstalta',lta=100e-3, sta=10e-3).data

    df = tr.stats.sampling_rate
    cft = recursive_sta_lta(tr.data, int(10e-3 * df), int(100e-3 * df))
    on_of = trigger_onset(cft, 5, 3)
    sr = tr.stats.sampling_rate
    st = tr.stats.starttime
    for of in on_of:
        sensors.append(tr.stats.station)
        on.append(st + of[0] / sr)
        off.append(st + of[1] / sr)


    # Plotting the results
    plt.figure()
    ax = plt.subplot(211)
    # plt.clf()
    plt.plot(tr.data, 'k')
    ymin, ymax = ax.get_ylim()
    print(len(on_of))
    if on_of != []:
        plt.vlines(on_of[:, 0], ymin, ymax, color='r', linewidth=2)
        plt.vlines(on_of[:, 1], ymin, ymax, color='b', linewidth=2)
    plt.subplot(212, sharex=ax)
    plt.plot(cft, 'k')
    plt.hlines([3, 2], 0, len(cft), color=['r', 'b'], linestyle='--')
    plt.axis('tight')
    plt.show()

    # input('bubu')
    plt.close('all')
