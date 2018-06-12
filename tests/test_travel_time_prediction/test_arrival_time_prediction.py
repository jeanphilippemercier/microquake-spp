from spp.travel_time import core
from spp.utils import get_stations
from importlib import reload
from datetime import datetime
from microquake.core import read
import numpy as np
import os

from microquake.waveform.pick import SNR_picker
import matplotlib.pyplot as plt

reload(core)


if __name__ == "__main__":

    location = "651280, 4767430, -140"
    # origin_time = datetime(2018, 4, 14, 19, 44, 22, 92372)
    stations = get_stations()

    data_dir = os.environ['SPP_DATA']
    config_dir = os.environ['SPP_CONFIG']

    st = read(os.path.join(data_dir, '2018-04-15_034422.mseed'),
               format='MSEED')

    buf = BytesIO()
    st.write(buf, format='MSEED')
    data = buf.getvalue()

    from spp.scripts import picker

    # testing the script receiving data in binary format
    cat = picker.predict_travel_time(location, data)




    # cat = core.create_event(st2, location)

    origin_time = cat[0].origins[0].time
    picks = cat[0].picks


    st = read(os.path.join(data_dir, '2018-04-15_034422.mseed'), format='MSEED')
    ##########
    # INSERT PICKER HERE, the picker function should take two non keyword
    # arguments (a stream and a catalogue and as many as keyword arguments as
    # you want. the keyword arguments will be passed from a config file
    # For instance in let say we get the parameter from a yaml file we would get
    # >> import yaml
    # >> params = yaml.load(...)
    # >> cat_out = picker(stream, catalogue, **params[picker][kwargs]
    # the SNR picker currently returns a catalogue but could probably simply
    # return a list of picks.
    # example of how I use the SNR picker below.
    # Note 1: The SNR picker is not optimized and does not currently work well.
    # Note 2: you will probably want to filter the waveforms. I have tried
    # filtering between 50 and 300 Hz. You may want to play more with the filter
    # Note, that the origin time should also be corrected following the
    # picking. Bias which will be the mean of the difference between the
    # initial picks and the new picks (Bias = new picks - initial picks) should
    #  be added to the origin time.



    (cat_picked, snr) = SNR_picker(st2.filter('bandpass', freqmin=50,
                                              freqmax=300), cat,
                                   SNR_dt=np.linspace(-10e-3, 10e-3, 100),
                                   SNR_window=(-5e-3, 20e-3))



    #########


    plt.clf()

    for tr in st.composite():
        for pk in cat_picked[0].picks:
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