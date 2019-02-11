
from helpers import *
import numpy as np
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
    arrivals_orig = {}
    for arr in origin.arrivals:
        picks.append(arr.pick_id.get_referred_object())
        pk = arr.pick_id.get_referred_object()
        sta = pk.waveform_id.station_code
        pha = pk.phase_hint
        if sta not in arrivals_orig:
            arrivals_orig[sta] = {}
        arrivals_orig[sta][pha] = arr

# MTH: create_arrivals_from_picks will create an entirely new set of arrivals (new resource_ids)
#      it will set arr.distance (looks exactly same as nlloc's arr.distance)
#      it will set arr.time_residual *** DIFFERS *** from arr.time_residual nlloc calcs/reads from last.hypo
#      it will fix the missing azim/theta that nlloc set to -1
#      it will drop nlloc arr.time_weight field

    arrivals = app.create_arrivals_from_picks(picks, ev_loc, origin.time)

    """
    for arr in arrivals:
        pk = arr.pick_id.get_referred_object()
        sta = pk.waveform_id.station_code
        pha = arr.phase
        arr_orig = arrivals_orig[sta][pha]
        print("sta:%s pha:%s orig_dist:%.2f [%.2f] orig_time_resid:%f [%f]" % \
              (sta,pha,arr_orig.distance, arr.distance, arr_orig.time_residual, arr.time_residual))

    exit()
    """

# Now set the receiver angles (backazimuth and incidence angle)

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

