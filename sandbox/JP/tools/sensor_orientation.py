from datetime import datetime
from microquake.IMS import web_client
from microquake.core import read_events
from microquake.core import UTCDateTime
from spp.utils import seismic_client
from spp.utils.application import Application
from microquake.simul import eik
from importlib import reload
import numpy as np
import pickle
import h5py
import logging
import tqdm
ev_info_logger = logging.getLogger('get_event_information')

def get_event_information(request_event, output_dir='data/'):
    event = request_event
    t0 = time()
    ev_info_logger.info('requesting data for event %s' % event.time_utc)
    cat = event.get_event()
    st = event.get_waveform()
    t1 = time()
    ev_info_logger.info('done requesting data for event %s in %0.3f seconds' %
                        (event.time_utc, (t1 - t0)))
    dict_seismo = {}
    for seismo in st:
        sta = seismo.stats.station
        filename = output_dir + '%s_%s.mseed' % (sta, event.time_utc)
        filename = filename.replace(r':', '_').replace('-', '_')
        dict_seismo[sta] = filename
        seismo.write(filename)
    for arrival in cat[0].preferred_origin().arrivals:
        pick = arrival.get_pick()
        if pick.phase_hint == 'S':
            continue
        key = pick.waveform_id.station_code
        seismogram_name = dict_seismo[key]
        p_time = pick.time
        yield (key, (seismogram_name, p_time, cat[0].preferred_origin().loc))


def get_traveltime_grid_station(station, phase='P'):
    """
    return the travel time grid for a specific station and a specific phase
    :param station: station name
    :param phase: phase id, 'P' or 'S' (not case sensitive), default = 'P'
    :return:
    """

    ####
    # To be changed, need to use the NLL grids instead of the microquake grids
    ###

    params = get_project_params()
    base_dir = params.nll['NLL_BASE']
    project_code = params.project_code


    from microquake.core import read_grid

    site = get_stations()
    stloc = site.select(station=station).stations()[0].loc

    try:
        phase = phase.lower()

        filename = '%s/time/%s.%s.%s.time' % (base_dir, project_code, phase,
                                                  station)

        tt_grid = read_grid(filename, format='NLLOC')

    except:
        phase = phase.upper()

        filename = '%s/time/%s.%s.%s.time' % (base_dir, project_code, phase,
                                                  station)

        tt_grid = read_grid(filename, format='NLLOC')

    tt_grid.seed = stloc

    return tt_grid


def sv_sh_orientation(P):

    inc_vect = P
    x_sh = np.linspace(-1, 1, 10000)
    dt_eq_1 = np.abs(inc_vect[0] * x_sh + inc_vect[1] * np.sqrt(1 - x_sh **
                                                              2))

    dt_eq_2 = np.abs(inc_vect[0] * x_sh - inc_vect[1] * np.sqrt(1 - x_sh **
                                                              2))

    mn_eq1 = np.min(dt_eq_1)
    mn_eq2 = np.min(dt_eq_2)

    if mn_eq1 <= mn_eq2:
        i = np.argmin(dt_eq_1)
        SH_vect = [x_sh[i], np.sqrt(1 - x_sh[i] ** 2), 0]
    else:
        i = np.argmin(dt_eq_2)
        SH_vect = [x_sh[i], -np.sqrt(1 - x_sh[i] ** 2), 0]

    SV_vect = np.cross(inc_vect, SH_vect) # not sure about the convention

    return (SV_vect, SH_vect)

def calculate_orientation_station(station, pick_dict, site, logger):

    base_url = "http://10.95.64.12:8002/ims-database-server/databases/mgl"
    network_code = 'OT'
    pick_array = pick_dict[station]

    sta = site.select(station=station).stations()[0]

    try:
        zorientation = sta.channels[2].orientation
    except:
        return (0, 0, 0)
    stloc = sta.loc

    N = 100

    Res = np.zeros(N**2)

    for kk, pick in enumerate(pick_array):
        logger.debug("processing station %s, event %d of %d, "
                     "" % (station, kk, len(pick_array)))
        evloc = pick[2]
        sgram_name = pick[0]
        pk_time = UTCDateTime(pick[1])
        # st = web_api.get_seismogram(base_url, sgram_name, network_code, station)
        st = pick_array['']
        st.detrend('demean').detrend('linear')
        st.filter('bandpass', freqmin=60, freqmax=1000)
        st2 = st.copy()
        starttime = pk_time
        endtime = pk_time + 0.015
        st.trim(starttime=starttime, endtime=endtime)
        if len (st) < 3:
            continue

        i_mx_x = np.argmax(np.abs(st[0].data))
        i_mx_y = np.argmax(np.abs(st[1].data))
        i_mx_z = np.argmax(np.abs(st[2].data))

        A = np.sqrt(st[0].data[i_mx_x] ** 2 + st[1].data[i_mx_y] ** 2 + st[
            2].data[i_mx_z] ** 2)
        # A_x = st[0].data[i_mx_x]
        # A_y = st[1].data[i_mx_y]
        # A_z = st[2].data[i_mx_z]

        A_x = np.std(st[0].data)
        A_y = np.std(st[1].data)
        cc_x_z = np.sign(np.corrcoef(st[0].data, st[2].data)[0, 1])
        cc_y_z = np.sign(np.corrcoef(st[1].data, st[2].data)[0, 1])

        x = np.linspace(-1, 1, N)
        y = np.linspace(-1, 1, N)

        X, Y = np.meshgrid(x, y)

        Xx = X.reshape(N ** 2)
        Yx = Y.reshape(N ** 2)

        X_Xz = Xx * zorientation[0]
        Y_Yz = Yx * zorientation[1]

        Zx = - (X_Xz + Y_Yz) / zorientation[2]

        Nx = np.sqrt(Xx ** 2 + Yx ** 2 + Zx ** 2)

        Xx = Xx[Nx > 0]
        Yx = Yx[Nx > 0]
        Zx = Zx[Nx > 0]
        Nx = Nx[Nx > 0]

        Xx = Xx / Nx
        Yx = Yx / Nx
        Zx = Zx / Nx

        X = np.array([([x_x, y_x, z_x] ) for (x_x, y_x, z_x) in zip(Xx, Yx,
                                                                    Zx)])
        z = zorientation
        # The sensors are right handed coordinate system
        Y = - np.array([np.cross(x, -z) for x in X])

        # reload(eik)
        #
        # ray = eik.ray_tracer(tt_grid, evloc, stloc)
        #
        # # finding the direction of the incident wave
        # inc_vects = np.diff(ray.nodes, axis=0)
        # inc_vect_norm = [inc_vect / np.linalg.norm(inc_vect) for inc_vect
        #                  in inc_vects]
        # inc_vect = inc_vect_norm[-2] # the last vector is reliable

        inc_vect = (stloc - evloc) / (np.linalg.norm(stloc - evloc))
        SV_vect, SH_vect = sv_sh_orientation(inc_vect)

        dot_inc_X = np.array([np.dot(inc_vect, x) for x in X])
        dot_inc_Y = np.array([np.dot(inc_vect, y) for y in Y])

        # res_x = dot_inc_X / (A_x / A * np.sign(np.dot(z, inc_vect)) *
        #                          cc_x_z)
        # res_y = dot_inc_Y / (A_y / A * np.sign(np.dot(z, inc_vect)) *
        #                          cc_y_z)

        P_ = [np.var(np.dot(x, inc_vect) * st[0].data + np.dot(y, inc_vect) *
             st[1].data + np.dot(z, inc_vect) * st[2].data)
             for x, y in zip(X, Y)]

        SV_ = [np.var(np.dot(x, SV_vect) * st[0].data + np.dot(y, SV_vect) *
             st[1].data + np.dot(z, SV_vect) * st[2].data)
             for x, y in zip(X, Y)]

        SH_ = [np.var(np.dot(x, SH_vect) * st[0].data + np.dot(y, SH_vect) *
             st[1].data + np.dot(z, SH_vect) * st[2].data)
             for x, y in zip(X, Y)]

        P_ = np.array(P_)
        SV_ = np.array(SV_)
        SH_ = np.array(SH_)
        # P_ = P_ / np.max(P_)

        # res = np.abs(res_x) + np.abs(res_y) + (SV_ + SH_) / P_
        #
        # res = (SV_ + SH_) / P_
        res = (SV_ + SH_) / P_
        res = 1 / res
        Res += res

        i = np.argmax(res)
        i2 = np.argmax(Res)

        x = X[i, :]
        x2 = X[i2, :]
        y = Y[i, :]
        y2 = Y[i2, :]

        logger.debug("x1: %s" % x)
        logger.debug("x2: %s" % x2)
        logger.debug("y1: %s" % y)
        logger.debug("y1: %s" % y2)
        logger.debug(res[i])
        logger.debug(Res[i2])



        # P = np.dot(x, inc_vect) * st2[0].data + np.dot(y, inc_vect) * st2[
        #     1].data + np.dot(z, inc_vect) * st2[2].data
        #
        #
        # # x = np.array([0.98039421, 0.19607884, 0.01950084])
        # # y = np.array([ 1.96228326e-01, -9.80539542e-01, -6.05393413e-03])
        #
        # SH = np.dot(x, SH_vect) * st2[0].data + np.dot(y, SH_vect) * st2[
        #      1].data + np.dot(z, SH_vect) * st2[2].data
        #
        # SV = np.dot(x, SV_vect) * st2[0].data + np.dot(y, SV_vect) * st2[
        #      1].data + np.dot(z, SV_vect) * st2[2].data
        #
        # import matplotlib.pyplot as plt
        #
        # # eig = np.linalg.eig([st[0].data,
        # #                      st[1].data,
        # #                      st[2].data])
        #
        # plt.close()
        # plt.figure(1)
        # plt.clf()
        # plt.subplot(311)
        # plt.plot(P)
        # plt.subplot(312)
        # plt.plot(SV)
        # plt.subplot(313)
        # plt.plot(SH)
        # plt.show()
        #
        # X_vects.append(x)
        # Y_vects.append(y)
        # Z_vects.append(z)
        #
        # st.trim(starttime=pk_time, endtime=pk_time+0.015)
        #
        # P = np.dot(x, inc_vect) * st[0].data + np.dot(y, inc_vect) * st[
        #     1].data + np.dot(z, inc_vect) * st[2].data
        #
        # SH = np.dot(x, SH_vect) * st[0].data + np.dot(y, SH_vect) * st[
        #      1].data + np.dot(z, SH_vect) * st[2].data
        #
        # SV = np.dot(x, SV_vect) * st[0].data + np.dot(y, SV_vect) * st[
        #      1].data + np.dot(z, SV_vect) * st[2].data
        #
        #
        # print((np.var(SH) + np.var(SV)) / np.var(P))
        #
        # S_in_P.append((np.var(SH) + np.var(SV)) / np.var(P))
        #
        #
        #
        # # Tracer()()

    i = np.argmax(Res)
    x = X[i, :]
    y = Y[i, :]


    return (x, y, z)


from time import time
app = Application()


site = app.get_stations()
tz = app.get_time_zone()
starttime = datetime(2018, 4, 1, tzinfo=tz)
endtime = datetime(2018, 11, 30, tzinfo=tz)
base_url = app.settings.seismic_api.base_url

# cat = web_api.get_catalogue(base_url, starttime, endtime, site, blast=True,
#                             get_arrivals=True)
logger = app.get_logger('sensor_orientation', 'sensor_orientation.log')
logger.info('requesting event list from the API')
t0 = time()
event_list = seismic_client.get_events_catalog(base_url, starttime, endtime)
t1 = time()
logger.info('done requesting event list. The API returned %d events in %0.3f seconds' % (len(event_list), (t1 - t0)))
# cat.write('events.xml', format='QUAKEML')

# read event data and write into H5 file

# for request_event in event_list:
#     logger.info('getting data for event: %s' % request_event.time_utc)
#     t0 = time()
#     st = request_event.get_waveform()
#     cat = request_event.get_event()
#     t1 = time()
#     logger.info('done getting data for event: %s in %0.3f' % (
#         request_event.time_utc, (t1 - t0)))
#     for tr in st:
#         group = f['/%s' % tr.stats.station].create_group(str(
#                 request_event.time_utc))
#
#         input('bubu')


# print('reading quakeml')
# cat = read_events('events.xml')
# cat = pickle.load(open('events.pickle', 'rb'))
print('done with reading the data')

# pick_dict = {}
# for tmp1 in map(get_event_information, tqdm(event_list)):
#     for tmp2 in tmp1:
#         if tmp2[0] in pick_dict.keys():
#             pick_dict[tmp2[0]].append(np.array(tmp2[1]))
#         else:
#             pick_dict[tmp2[0]] = [np.array(tmp2[1])]
#
# pickle.dump(pick_dict, open('pick_dict.pickle', 'wb'))

pick_dict = pickle.load(open('pick_dict.pickle', 'rb'))
# input('aqui')

try:
    orientation = pickle.load(open('orientation.pickle', 'rb'))

except:
    orientation = {}


for key in pick_dict.keys():
    # if key in orientation.keys():
    #     continue
    print('Processing %s' % key)
    try:
        x, y, z = calculate_orientation_station(key, pick_dict, site)
    # if x == 0:
    #     continue
    except:
        continue
    orientation[key] = {}
    orientation[key]['x'] = x
    orientation[key]['y'] = y
    orientation[key]['z'] = z
    orientation[key]['measurements'] = len(pick_dict[key])

    with open('orientation.pickle', 'wb') as fo:
        pickle.dump(orientation, fo)

st_ids = np.arange(1, 110)

with open('orientation.csv', 'w') as fo:
    for st_id in st_ids:
        key = str(st_id)
        if not key in orientation.keys():
            fo.write("%s,z,0,90\n" % st_id)
            continue
        print(key)
        for k, station in enumerate(site.networks[0].stations):
            if site.networks[0].stations[k].code == key:
                if len(site.networks[0].stations[k].channels) == 1:
                    z = site.networks[0].stations[k].channels[0]
                    fo.write("%s,z,%f,%f\n" % (st_id, z.azimuth,
                                          z.dip))
                else:
                    site.networks[0].stations[k].channels[0].orientation =  \
                        orientation[key]['x']
                    site.networks[0].stations[k].channels[1].orientation = \
                        orientation[key]['y']
                    x = site.networks[0].stations[k].channels[0]
                    y = site.networks[0].stations[k].channels[1]
                    z = site.networks[0].stations[k].channels[2]
                    fo.write("%s,x,%f,%f,y,%f,%f,z,%f,%f\n" \
                             % (st_id, x.azimuth, x.dip,
                                y.azimuth, y.dip, z.azimuth, z.dip))





# next need to write in a file.


# st = web_api.get_seismogram(base_url, sgram_name, network_code, site_code)


# sc = SparkContext('local', 8)

# rdd = sc.parallelize(cat)

#st = web_api.get_seismogram_event(base_url, cat[10], 'OT')

