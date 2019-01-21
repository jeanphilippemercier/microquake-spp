import os
import numpy as np

import warnings
warnings.simplefilter("ignore", UserWarning)

from obspy.core.event import read_events
#from helpers import *
#import matplotlib.pyplot as plt

from spp.utils.application import Application
from microquake.core import read
from microquake.core.event import read_events as micro_read_events
from microquake.core.event import (Origin, CreationInfo, Event)

def main():

    # reading application data
    app = Application()
    settings = app.settings
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    event_file = data_dir + "20180706112101.xml"
    mseed_file = event_file.replace('xml','mseed')

    # Read in using obspy read_events and copy
    event  = read_events(event_file)[0]
    event_new  = event.copy()
    print(event_new)

    # Read in using microquake read_events and copy
    micro_event  = micro_read_events(event_file)[0]
    micro_event.magnitudes[0].corner_frequency = 111.111

    print("************* Now copy it ***************")
    new_event = micro_event.copy()
    new_event.write("new_event.xml", "QUAKEML")
    exit()

    #new_event = copy.deepcopy(event)
    micro_event.picks = []
    micro_event.origins[0].arrivals = []
    #new_event = copy.deepcopy(micro_event)
    micro_event.origins[0].__dict__['warn_on_non_default_key'] = False
    micro_event.__dict__['warn_on_non_default_key'] = False
    new_event = micro_event.copy()
    exit()

    picks = event.picks
    print(type(picks[0]))
    '''
    for pick in picks:
        pick.backazimuth=180.
        pick.method = 'IMS'
        print(pick)
        new_pick = pick.copy()
        exit()
    exit()
    '''

    print(event.LOCATION_X)
    #new_event = event.copy()
    #print(new_event)
    #exit()

    origin = event.origins[0]
    print(origin.loc)

    st = read(mseed_file, format='MSEED')
    #st = Stream(read(mseed_file, format='MSEED'))

    evloc = origin.loc

    sensor_csv = os.environ['SPP_COMMON'] + '/sensors.csv'
    inv = get_inventory(sensor_csv)

    for pick in event.picks:
        '''
        pick.method = "IMS"
        #pick.extra.warn_on_non_default_key = False
        pick.extra['warn_on_non_default_key'] = False
        pick.extra['do_not_warn_on'] = ['method', 'snr']
        '''
        '''
        for k,v in pick['extra'].items():
            print(k)
        pick.backazimuth=180.
        print(pick.backazimuth)
        pick.warn_on_non_default_key = False
        print(pick.warn_on_non_default_key)
        #pick.__setattr__(warn_on_non_default_key, False)
        #pick.do_not_warn_on += ['snr', 'trace_id']
        #print((pick.do_not_warn_on))
        #print(type(pick.do_not_warn_on))
        #pick.warn_on_non_default_key = False
        '''

        print("Now if the time to ... copy")
        print("copy from pick type=%s" % type(pick))
        new_pick = pick.copy()
        exit()
        #print(pick.extra)
    exit()
    plot_profile_with_picks(st, picks=event.picks, origin=origin, title="IMS picks")

    pick_dict = copy_picks_to_dict(event.picks)

    arr_P = [ arr for arr in new_origin.arrivals if \
              arr.pick_id.get_referred_object().phase_hint == 'P' ]
    arr_S = [ arr for arr in new_origin.arrivals if \
              arr.pick_id.get_referred_object().phase_hint == 'S' ]

# 1. Create a dict of keys=sta_code for all 'P' arrivals with the necessary pick/sta info inside
    sta_dict = {}
    for arr in arr_P:
        pick = arr.pick_id.get_referred_object()
        sta_code = pick.waveform_id.station_code
        d = {}
        d['ptime']    = pick.time

        if 'S' in pick_dict[sta_code]:
            d['stime']   = pick_dict[sta_code]['S'].time
        else:
            d['stime']   = core.get_travel_time_grid_point(sta_code, new_origin.loc, phase='S', use_eikonal=False)

        found = False
        for sta in inv[0].stations:
            if sta.code == sta_code:
                d['loc'] = sta.loc
                found = True
                break
        if not found:
            print("Oops: sta:%s not found in inventory!" % sta_code)
            raise

        d['R'] = np.linalg.norm(d['loc'] - evloc) # Dist in meters
        sta_dict[sta_code] = d

# Are we fitting displacement (True) or velocity (False) spectra ?
    fit_displacement = True
    fit_displacement = False

    if fit_displacement:
        model_func = brune_dis_spec
    else:
        model_func = brune_vel_spec


# 2. Calc/save signal/noise fft spectra at all channels that have P arrivals:
    for sta_code,sta in sta_dict.items():

        signal_start = sta['ptime'] - .01
        signal_end   = sta['stime'] - .01
        signal_len   = signal_end - signal_start

        noise_end   = sta['ptime'] - .01
        noise_start = noise_end - signal_len

        nfft = 512
        nfft = 1024
        dt = 1/6000.
        df = 1/(float(nfft)*dt)
        trs = st.select(station=sta_code)
        if trs:
            chans = {}

            for tr in trs:
                ch = {}
                tt_s = sta['stime'] - tr.stats.starttime
                tr.detrend('demean').detrend('linear').taper(type='cosine', max_percentage=0.05, side='both')

                signal = tr.copy()
                signal.trim(starttime=signal_start, endtime=signal_end)
                signal.detrend('demean').detrend('linear')
                signal.taper(type='cosine', max_percentage=0.05, side='both')
                if fit_displacement:
                    signal.integrate().taper(type='cosine', max_percentage=0.05, side='both')

                noise  = tr.copy()
            # if noise_start < trace.stats.starttime - then what ?
                noise.trim(starttime=noise_start, endtime=noise_end)
                noise.detrend('demean').detrend('linear')
                noise.taper(type='cosine', max_percentage=0.05, side='both')
                if fit_displacement:
                    noise.integrate().taper(type='cosine', max_percentage=0.05, side='both')

                (signal_fft, freqs) = unpack_rfft( rfft(signal.data, n=nfft), df)
                (noise_fft, freqs)  = unpack_rfft( rfft(noise.data, n=nfft), df)
                #signal_fft = smooth(signal_fft, window_len=9, window='hanning')[4:-4]
                #noise_fft  = smooth(noise_fft , window_len=9, window='hanning')[4:-4]

            # np/scipy ffts are not scaled by dt
            # Do it here so we don't forget 
                signal_fft *= dt
                noise_fft  *= dt

                #plot_signal(signal, noise)

                ch['nfft'] = nfft
                ch['dt']   = dt
                ch['df']   = df
                ch['freqs'] = freqs
                ch['signal_fft'] = signal_fft
                ch['noise_fft']  = noise_fft
                if fit_displacement:
                    ch['integrated'] = True
                else:
                    ch['integrated'] = False
                chans[tr.stats.channel] = ch
            sta['chan_spec'] = chans


# 2. Grid search of best source fc by optimizing fit to all sta/cha specs

    best_fc = 0.
    best_fit = 1e12

    #for fc in [20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150]:
    #for fc in [30, 80, 120, 150]:
    for fc in [80, 90, 95, 100, 105, 110, 120]:
        Mws = []
        fit = 0.
        #print("fc=%.2f" % fc)
        for sta_code, sta in sta_dict.items():
            #print("  sta:%3s" % sta_code)
            if 'chan_spec' not in sta:
                print("  sta:%3s: Has no chan_spec --> Skip" % sta_code)
                continue
            for cha_code, cha in sta['chan_spec'].items():
                    #continue
                    #print("    Process: cha:%s" % cha_code)
                    signal_fft, freqs, noise_fft = (cha['signal_fft'], cha['freqs'], cha['noise_fft'])


                    # Optimize fit of model_func to signal_fft to solve for smom:

                    residfit = getresidfit(np.abs(signal_fft), model_func, fc, freqs, Lnorm='L2', \
                                           weight_fr=False, fmin=20., fmax=1e3)

                    # Give it a starting vector:
                    #     smom    tstar
                    pp = [1e-5, tt_s/500.]
                    sol = fmin(residfit, np.array(pp),xtol=10**-12,ftol=10**-6,disp=False)
                    if sol[0] < 0. :
                        print("Ummm smom < 0 !!!")

                    # Similar result:
                    #sol2 = minimize(residfit, np.array(pp), method='Nelder-Mead')
                    #print(sol2.success)
                    #print(sol2.x)
                    # Gives fucked up result:
                    #sol3 = minimize(residfit, np.array(pp), method='L-BFGS-B', bounds=((1e-6,1e2),(1e-12,1e12)), \
                                    #options={'gtol':1e-12, 'ftol':1e-12})
                    #print(sol3.success)
                    #print(sol3.x)

                    #fi = np.nonzero((freqs >= 20) & (freqs <= 1000))[0]
                    #popt, pcov = curve_fit(SF, freqs[fi], np.log10( np.abs(signal_fft[fi]) + 1e-10))
                    #print(popt)
                    #exit()

                    plot_fit = 1
                    if plot_fit:
                        model_spec = np.array( np.zeros(freqs.size), dtype=np.float_)
                        for i,f in enumerate(freqs):
                            model_spec[i] = model_func(fc, sol[0], sol[1], f)
                        #y1 = np.abs(signal_fft)
                        #y2 = smooth(np.abs(signal_fft), window_len=3)
                        #print("y1.size=%d y2.size=%d" % (y1.size, y2.size))
                        plot_spec(freqs, signal_fft, noise_fft, model_spec, title="sta:%s ch:%s spec fit" % \
                                  (sta_code, cha_code))

                    ch_fit = residfit(sol)
                    #print("    ch:%s fit:%12.8g" % (cha_code, ch_fit))
                    fit += ch_fit

                    lambda_i = sol[0] * sta['R']
                    rho= 2700.
                    v= 5200.
                    rad = 0.52
                    scale = 4. * np.pi * rad * rho * v**3
                    M0 = lambda_i*scale
                    Mw = 2./3. * np.log10(M0) - 6.0333
                    Mws.append(Mw)
                    #print("sta:%3s ch:%s Mw:%.2f" % (sta_code, cha_code, Mw))
                    #exit()
            exit()

        print("fc:%5.2f fit:%12.8g Mw:%.2f" % (fc, fit, np.median(Mws)))
        if fit < best_fit:
            best_fit = fit
            best_fc = fc


    print("best_fc:%5.2f" % best_fc)
    exit()
    '''
                t = []
                for i in range(signal.stats.npts ):
                    t.append(float(i)*dt)
                plt.plot(t, signal, color='blue')
                plt.plot(t, noise, color='red')
                plt.legend(['signal', 'noise'])
                plt.title("%s: P window" % signal.get_id())
                plt.grid()
                plt.show()

                #plt.semilogy(freqs, np.abs(signal_fft), color='blue')
                #plt.semilogy(freqs, np.abs(noise_fft), color='red')
                plt.loglog(freqs, np.abs(signal_fft), color='blue')
                plt.loglog(freqs, np.abs(noise_fft),  color='red')
                plt.loglog(freqs, model_spec,  color='green')
                plt.legend(['signal', 'noise'])
                plt.xlim(1e0, 1e3)
                plt.ylim(1e-12, 1e-6)
                plt.title("%s: P spec" % signal.get_id())
                plt.grid()
                plt.show()
                #exit()

            #exit()
    '''

def plot_spec(freqs, signal_fft, noise_fft, model_spec, title=None):

    plt.loglog(freqs, np.abs(signal_fft), color='blue')
    plt.loglog(freqs, np.abs(noise_fft),  color='red')
    plt.loglog(freqs, model_spec,  color='green')
    plt.legend(['signal', 'noise', 'model'])
    plt.xlim(1e0, 3e3)
    #plt.ylim(1e-12, 1e-4)
    if title:
        plt.title(title)
    plt.grid()
    plt.show()

def plot_signal(signal, noise):
    '''
        signal = obspy Trace windowed around signal
        noise  = obspy Trace windowed around pre-signal noise
    '''
    t = []
    dt = signal.stats.sampling_rate
    for i in range(signal.stats.npts ):
        t.append(float(i)*dt)
    plt.plot(t, signal, color='blue')
    plt.plot(t, noise, color='red')
    plt.legend(['signal', 'noise'])
    plt.title("%s: P window" % signal.get_id())
    plt.grid()
    plt.show()

def unpack_rfft(rfft, df):
    n = rfft.size
    if n % 2 == 0:
        n2 = int(n/2)
    else:
        print("n is odd!!")
        exit()
    #print("n2=%d" % n2)

    c_arr = np.array( np.zeros(n2+1,), dtype=np.complex_)
    freqs = np.array( np.zeros(n2+1,), dtype=np.float_)

    c_arr[0]  = rfft[0]
    c_arr[n2] = rfft[n-1]
    freqs[0]  = 0.
    freqs[n2] = float(n2)*df

    for i in range(1, n2):
        freqs[i] = float(i)*df
        c_arr[i] = np.complex(rfft[2*i - 1], rfft[2*i])

    return c_arr, freqs


import numpy

def smooth(x,window_len=11,window='hanning'):
    """smooth the data using a window with requested size.

    This method is based on the convolution of a scaled window with the signal.
    The signal is prepared by introducing reflected copies of the signal
    (with the window size) in both ends so that transient parts are minimized
    in the begining and end part of the output signal.

    input:
        x: the input signal
        window_len: the dimension of the smoothing window; should be an odd integer
        window: the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'
            flat window will produce a moving average smoothing.

    output:
        the smoothed signal

    example:

    t=linspace(-2,2,0.1)
    x=sin(t)+randn(len(t))*0.1
    y=smooth(x)

    see also:

    numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
    scipy.signal.lfilter

    TODO: the window parameter could be the window itself if an array instead of a string
    NOTE: length(output) != length(input), to correct this: return y[(window_len/2-1):-(window_len/2)] instead of just y.
    """

    if x.ndim != 1:
        raise(ValueError, "smooth only accepts 1 dimension arrays.")
        #raise ValueError, "smooth only accepts 1 dimension arrays."

    if x.size < window_len:
        raise(ValueError, "Input vector needs to be bigger than window size.")
        #raise ValueError, "Input vector needs to be bigger than window size."


    if window_len<3:
        return x


    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise(ValueError, "Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")
        #raise ValueError, "Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'"


    s=numpy.r_[x[window_len-1:0:-1],x,x[-2:-window_len-1:-1]]
    #print(len(s))
    if window == 'flat': #moving average
        w=numpy.ones(window_len,'d')
    else:
        w=eval('numpy.'+window+'(window_len)')

    y=numpy.convolve(w/w.sum(),s,mode='valid')
    return y



if __name__ == '__main__':

    main()
