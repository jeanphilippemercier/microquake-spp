
from helpers import *
import matplotlib.pyplot as plt

import os
import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from obspy.core.event.base import ResourceIdentifier

from microquake.core import read
from microquake.core import UTCDateTime
from microquake.core.event import read_events as read_events
from microquake.core.event import (Origin, CreationInfo, Event)
from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.waveform.amp_measures import measure_pick_amps, measure_velocity_pulse, set_pick_snrs

from microquake.core.util.tools import copy_picks_to_dict

from spp.utils.application import Application

def fix_arr_takeoff_and_azimuth(cat_out, sta_meta_dict, app=None):

    # app.get_grid_point(.. type={'time' [default], 'take_off', 'azimuth') - reads/interpolates the
    #    corresponding files in common/NLL/time.  Note that take_off/azimuth are calculated by JP 
    #    when 00_prepare..py is run by computing grads of Grid2Time *.time.buf grids


    event = cat_out[0]
    origin = event.preferred_origin()
    ev_loc = origin.loc
    #ev_loc = np.array( [651121, 4767056, -479] )  # 400 m below station 41
    #ev_loc = np.array( [651121, 4767156, -479] )  # 400 m below station 41 and 100m N
    #ev_loc = np.array( [651221, 4767156, -479] )  # 400 m below station 41 and 100m N and 100m E
    #ev_loc = np.array( [651021, 4767056, -79] )  # 100 m to left of station 41
    #ev_loc = np.array( [651121, 4767156, -79] )  # 100 m North of station 41

    vp_grid, vs_grid = app.get_velocities()
    vp = vp_grid.interpolate(ev_loc)[0]
    vs = vs_grid.interpolate(ev_loc)[0]

    picks = []
    for arr in origin.arrivals:
        picks.append(arr.pick_id.get_referred_object())

    arrivals = app.create_arrivals_from_picks(picks, ev_loc, origin.time)

    for arr in arrivals:
        pk = arr.pick_id.get_referred_object()
        sta = pk.waveform_id.station_code
        pha = arr.phase

        st_loc = sta_meta_dict[sta]['station'].loc
        xoff = ev_loc[0]-st_loc[0]
        yoff = ev_loc[1]-st_loc[1]
        zoff = np.abs(ev_loc[2]-st_loc[2])
        H = np.sqrt(xoff*xoff + yoff*yoff)
        alpha = np.arctan2(zoff,H)
        beta  = np.pi/2. - alpha
        takeoff_straight = alpha * 180./np.pi + 90.
        inc_straight = beta * 180./np.pi

        if pha == 'P':
            v = vp
            v_grid = vp_grid
        elif pha == 'S':
            v = vs
            v_grid = vs_grid

        p = np.sin(arr.takeoff_angle*np.pi/180.) / v

        v_sta = v_grid.interpolate(st_loc)[0]

        inc_p  = np.arcsin(p*v_sta) * 180./np.pi

        # I have the incidence angle now, need backazimuth so rotate to P,SV,SH
        back_azimuth = np.arctan2(xoff,yoff) * 180./np.pi
        if back_azimuth < 0:
            back_azimuth += 360.

        arr.backazimuth = back_azimuth
        arr.inc_angle   = inc_p

        print("%3s: [%s] takeoff:%6.2f [stx=%6.2f] inc_p:%.2f [inc_stx:%.2f] baz:%.1f [az:%.1f]" % \
              (sta, arr.phase, arr.takeoff_angle, takeoff_straight, \
               inc_p, inc_straight, back_azimuth, arr.azimuth))

    cat_out[0].preferred_origin().arrivals = arrivals

    return

def rotate_to_P_SV_SH(st, cat):

    fname = 'rotate_to_P_SV_SH'

    st_new = st.copy()

    event = cat[0]
    for arr in event.preferred_origin().arrivals:
        if arr.phase == 'S':
            continue

        pk = arr.pick_id.get_referred_object()
        sta = pk.waveform_id.station_code
        baz = arr.backazimuth
        az = arr.azimuth
        takeoff = arr.takeoff_angle
        inc_angle = arr.inc_angle

        trs = st_new.select(station=sta)
        if len(trs) == 3:

            cos_i = np.cos(inc_angle * np.pi/180.)
            sin_i = np.sin(inc_angle * np.pi/180.)
            cos_baz = np.cos(baz * np.pi/180.)
            sin_baz = np.sin(baz * np.pi/180.)

            col1 = np.array([cos_i, sin_i, 0.])
            col2 = np.array([-sin_i*sin_baz, cos_i*sin_baz, -cos_baz])
            col3 = np.array([-sin_i*cos_baz, cos_i*cos_baz, sin_baz])

            A = np.column_stack((col1,col2,col3))

            print("sta:%s az:%.1f baz:%.1f takeoff:%.1f inc:%.1f" % (sta, az, baz, takeoff, inc_angle))

            E = trs[0].data
            N = trs[1].data
            Z = trs[2].data
            D = np.row_stack((Z,E,N))

            foo = A @ D

            #if sta in ['59', '87']:
                #trs.plot()


            trs[0].data = foo[0,:]
            trs[1].data = foo[1,:]
            trs[2].data = foo[2,:]
            trs[0].stats.channel='P'
            trs[1].stats.channel='SV'
            trs[2].stats.channel='SH'

            '''
            P = trs[0].copy().trim(starttime = pk.time -.02, endtime=pk.time +.02)
            SV = trs[1].copy().trim(starttime = pk.time -.02, endtime=pk.time +.02)
            SH = trs[2].copy().trim(starttime = pk.time -.02, endtime=pk.time +.02)

            S = np.sqrt(SV.data**2 + SH.data**2)
            print(type(S))
            print(S)

            PtoS = np.var(P.data)/np.var(S)
            print(type(PtoS))
            print(PtoS)

            print("P_max:%g SV_max:%g SH_max:%g P/S:%f" % (np.max(np.abs(P.data)), np.max(np.abs(SV.data)), \
                                                    np.max(np.abs(SH.data), PtoS)))

            #if sta in ['59', '87']:
                #trs.plot()
            #exit()
            '''


        else:
            print("sta:%s --> only has n=%d traces --> can't rotate" % (sta, len(trs)))

    return st_new


def rotate_to_ENZ(st, sta_meta_data):
    st_new = st.copy()

    for sta in st_new.unique_stations():

        trs = st_new.select(station=sta)

        if len(trs) == 3:
            #trs.plot()

            col1 = sta_meta_data[sta]['chans']['x'].cosines
            col2 = sta_meta_data[sta]['chans']['y'].cosines
            col3 = sta_meta_data[sta]['chans']['z'].cosines
            A = np.column_stack((col1,col2,col3))
            At = A.transpose()
            #print(A)
            #print(At)

            x = trs[0].data
            y = trs[1].data
            z = trs[2].data
            D = np.row_stack((x,y,z))

            foo = At @ D

            #print(At.shape)
            #print(D.shape)
            #print(foo.shape)

            trs[0].data = foo[0,:]
            trs[1].data = foo[1,:]
            trs[2].data = foo[2,:]
            trs[0].stats.channel='E'
            trs[1].stats.channel='N'
            trs[2].stats.channel='Z'

            #trs.plot()


    return st_new

