from datetime import datetime
from obspy import UTCDateTime
from microquake.core import read
import numpy as np
from tqdm import tqdm
from pathos.multiprocessing import ProcessingPool
from microquake.core.settings import settings
from microquake.core.helpers.time import get_time_zone
from microquake.core.helpers.grid import get_grid
from spp.core.helpers.logging import logger
from spp.clients import api_client
from time import time
import pickle


def get_event_information(request_event, output_dir='data/'):
    event = request_event
    t0 = time()
    logger.info('requesting data for event %s' % event.time_utc)
    cat = event.get_event()
    st = event.get_waveforms()
    t1 = time()
    logger.info('done requesting data for event %s in %0.3f seconds' %
                        (event.time_utc, (t1 - t0)))
    dict_seismo = {}

    for station in st.unique_stations():

        seismo = st.select(station=station)
        filename = output_dir + '%s_%s.mseed' % (station, event.time_utc)
        filename = filename.replace(r':', '_').replace('-', '_')
        dict_seismo[station] = filename
        seismo.write(filename)

    for arrival in cat[0].preferred_origin().arrivals:
        pick = arrival.get_pick()
        if pick.phase_hint == 'S':
            continue
        key = pick.waveform_id.station_code
        seismogram_name = dict_seismo[key]
        p_time = pick.time
        yield (key, (seismogram_name, p_time, cat[0].preferred_origin().loc))


def get_travel_time_grid_station(station, phase='P'):
    """
    return the travel time grid for a specific station and a specific phase
    :param station: station name
    :param phase: phase id, 'P' or 'S' (not case sensitive), default = 'P'
    :return:
    """

    ####
    # To be changed, need to use the NLL grids instead of the microquake grids
    ###

    st_loc = inventory.select(station)[0].loc

    tt_grid = get_grid(station, phase.upper())

    tt_grid.seed = st_loc

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


def calculate_orientation_sensor(station, pick_dict, inventory):

    pick_array = pick_dict[station]

    station = inventory.select(station)

    if len(station.channels) < 3:
        return station.channels[0].cosines

    z_orientation = station[2].cosines

    st_loc = station.loc

    n = 100

    res = np.zeros(n ** 2)

    for kk, pick in enumerate(tqdm(pick_array)):
        logger.debug("processing station %s, event %d of %d, "
                     "" % (station, kk, len(pick_array)))
        ev_loc = pick[2]
        pk_time = UTCDateTime(pick[1])
        st = read(pick[0], format='MSEED')
        st.detrend('demean').detrend('linear')
        st.filter('bandpass', freqmin=100, freqmax=1000)
        starttime = pk_time
        endtime = pk_time + 0.015
        st.trim(starttime=starttime, endtime=endtime)
        if len (st) < 3:
            continue

        i_mx_x = np.argmax(np.abs(st[0].data))
        i_mx_y = np.argmax(np.abs(st[1].data))
        i_mx_z = np.argmax(np.abs(st[2].data))

        a = np.sqrt(st[0].data[i_mx_x] ** 2 + st[1].data[i_mx_y] ** 2 + st[
            2].data[i_mx_z] ** 2)
        a_x = np.std(st[0].data)
        a_y = np.std(st[1].data)
        cc_x_z = np.sign(np.corrcoef(st[0].data, st[2].data)[0, 1])
        cc_y_z = np.sign(np.corrcoef(st[1].data, st[2].data)[0, 1])

        x = np.linspace(-1, 1, n)
        y = np.linspace(-1, 1, n)

        X, Y = np.meshgrid(x, y)

        Xx = X.reshape(n ** 2)
        Yx = Y.reshape(n ** 2)

        X_Xz = Xx * z_orientation[0]
        Y_Yz = Yx * z_orientation[1]

        Zx = - (X_Xz + Y_Yz) / z_orientation[2]

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
        z = z_orientation
        # The sensors are right handed coordinate system
        Y = - np.array([np.cross(x, z) for x in X])

        inc_vect = (st_loc - ev_loc) / (np.linalg.norm(st_loc - ev_loc))
        SV_vect, SH_vect = sv_sh_orientation(inc_vect)

        dot_inc_X = np.array([np.dot(inc_vect, x) for x in X])
        dot_inc_Y = np.array([np.dot(inc_vect, y) for y in Y])

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

        res = (SV_ + SH_) / P_
        res = 1 / res
        res += res

        i = np.argmax(res)
        i2 = np.argmax(res)

        x = X[i, :]
        x2 = X[i2, :]
        y = Y[i, :]
        y2 = Y[i2, :]

        logger.debug("x1: %s" % x)
        logger.debug("x2: %s" % x2)
        logger.debug("y1: %s" % y)
        logger.debug("y1: %s" % y2)
        logger.debug(res[i])
        logger.debug(res[i2])

    i = np.argmax(res)
    x = X[i, :]
    y = Y[i, :]

    return x, y, z, station, len(pick_array)


# TODO This should be turned into a function that takes
# 1) a start time
# 2) an end time
# 3) The list for sensor for which the orientation need to be calculated
# The orientation of the sensors is given by the orientation dictionary

inventory = settings.inventory

tz = get_time_zone()
starttime = datetime(2018, 11, 29, tzinfo=tz)
endtime = datetime(2018, 11, 30, tzinfo=tz)
base_url = settings.get('API_BASE_URL')

logger.info('requesting event list from the API')
t0 = time()
event_list = api_client.get_events_catalog(base_url, starttime, endtime)
t1 = time()
logger.info('done requesting event list. '
            'The API returned %d events in %0.3f seconds'
            % (len(event_list), (t1 - t0)))

print('done with reading the data')

logger.info('building picking dictionary')
pick_dict = {}
for tmp1 in map(get_event_information, tqdm(event_list)):
    for tmp2 in tmp1:
        if tmp2[0] in pick_dict.keys():
            pick_dict[tmp2[0]].append(np.array(tmp2[1]))
        else:
            pick_dict[tmp2[0]] = [np.array(tmp2[1])]

    orientation = {}


pool = ProcessingPool(nodes=10)

# TODO replace sensor_id_list by the list provided by the user, note that if
#  the list is empty, the orientation is calculated for all sensors
sensor_id_list = []
fun = lambda x: calculate_orientation_sensor(x, pick_dict, inventory)

sensor_ids = []
if not sensor_id_list:
    sensor_ids = pick_dict.keys()
else:
    sensor_ids = [s_id for s_id in pick_dict.keys() if s_id in sensor_id_list]

results = pool.map(fun, sensor_ids)

# Orientation is the orientation vectors for all the stations.
# TODO
for r in results:
    try:
        sta = r[3].code
        orientation[sta] = {}
        # the following are
        orientation[sta]['x'] = r[0]
        orientation[sta]['y'] = r[1]
        orientation[sta]['z'] = r[2]
        orientation[sta]['measurements'] = r[4]
    except:
        continue

# TODO need to remove all the temporary files
