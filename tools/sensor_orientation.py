from spp.utils import get_stations, get_project_params
from spp.time import get_time_zone
from datetime import datetime

from microquake.IMS import web_api
from microquake.core import read_events
from microquake.spark import mq_map
from microquake.core import UTCDateTime

from microquake.simul import eik

import numpy as np
from IPython.core.debugger import Tracer
from importlib import reload
import numpy as np
import pickle

# from pyspark import SparkContext


def get_event_information(event):
    tmp = event.ASSOC_SEISMOGRAM_NAMES[1:-1].split(';')
    dict_seismo = {}
    for seismo in tmp:
        sta = seismo.split('_')[2]
        dict_seismo[sta] = seismo
    for pick in event.picks:
        if pick.phase_hint == 'S':
            continue
        key = pick.waveform_id.station_code
        key_s = str.zfill(key, 4)
        seismogram_name = dict_seismo[key_s]
        p_time = pick.time
        yield (key, (seismogram_name, p_time, event.preferred_origin().loc))


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

    phase = phase.lower()

    filename = '%s/time/%s.%s.%s.time' % (base_dir, project_code, phase,
                                              station)

    site = get_stations()
    stloc = site.select(station=station).stations()[0].loc
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


def calculate_orientation_station(station, pick_dict, site):

    base_url = "http://10.95.64.12:8002/ims-database-server/databases/mgl"
    network_code = 'OT'
    pick_array = pick_dict[station]

    sta = site.select(station=station).stations()[0]
    zorientation = sta.channels[2].orientation
    stloc = sta.loc

    X_vects = []
    Y_vects = []
    Z_vects = []
    S_in_P = []

    Res = []

    for pick in pick_array:
        evloc = pick[2]
        sgram_name = pick[0]
        pk_time = UTCDateTime(pick[1])
        st = web_api.get_seismogram(base_url, sgram_name, network_code, station)
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


        tt_grid = get_traveltime_grid_station(station)

        N = 100

        x = np.linspace(-1, 1, N)
        y = np.linspace(-1, 1, N)

        X, Y = np.meshgrid(x, y)

        Xx = X.reshape(N ** 2)
        Yx = Y.reshape(N ** 2)

        X_Xz = Xx * zorientation[0]
        Y_Yz = Yx * zorientation[1]

        Zx = -(X_Xz + Y_Yz)/zorientation[2]

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
        Y = np.array([np.cross(x, z) for x in X])

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

        res_x = dot_inc_X / (A_x / A * np.sign(np.dot(z, inc_vect)) *
                                 cc_x_z)
        res_y = dot_inc_Y / (A_y / A * np.sign(np.dot(z, inc_vect)) *
                                 cc_y_z)

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
        Res.append(res)

        i = np.argmax(res)

        x = X[i, :]
        y = Y[i, :]

        print(x)
        print(y)


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

    i = np.argmax(np.sum(Res,axis=0))
    x = X[i, :]
    y = Y[i, :]


    return (x, y, z)


base_url = "http://10.95.64.12:8002/ims-database-server/databases/mgl"

site = get_stations()
tz = get_time_zone()
endtime = datetime.now().replace(tzinfo=tz)
starttime = datetime(2018, 4, 1, tzinfo=tz)

# cat = web_api.get_catalogue(base_url, starttime, endtime, site, blast=True,
#                             get_arrivals=True)
#
# cat.write('events.xml', format='QUAKEML')

# print('reading quakeml')
# # cat = read_events('events.xml')
# cat = pickle.load(open('events.pickle', 'rb'))
#
# pick_dict = {}
# for tmp1 in map(get_event_information, cat):
#     for tmp2 in tmp1:
#         if tmp2[0] in pick_dict.keys():
#             pick_dict[tmp2[0]].append(np.array(tmp2[1]))
#         else:
#             pick_dict[tmp2[0]] = [np.array(tmp2[1])]

pick_dict = pickle.load(open('pick_dict.pickle', 'rb'))

#'59'
try:
    orientation = pickle.load(open('orientation.pickle', 'rb'))

except:
    orientation = {}


for key in pick_dict.keys():
    if key in orientation.keys():
        continue
    print('Processing %s' % key)
    x, y, z = calculate_orientation_station(key, pick_dict, site)
    orientation[key] = {}
    orientation[key]['x'] = x
    orientation[key]['y'] = y
    orientation[key]['z'] = z

    with open('orientation.pickle', 'wb') as fo:
        pickle.dump(orientation, fo)

# next need to write in a file.


# st = web_api.get_seismogram(base_url, sgram_name, network_code, site_code)


# sc = SparkContext('local', 8)

# rdd = sc.parallelize(cat)

#st = web_api.get_seismogram_event(base_url, cat[10], 'OT')

