
import os
import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from obspy.core.event.base import ResourceIdentifier

from microquake.core import read
from microquake.core.event import read_events as read_events
from microquake.waveform.mag_new import calc_magnitudes_from_lambda, set_new_event_mag

from spp.utils.application import Application

from lib_process import *
from helpers import *


def main():

    fname = 'calc_moment_magnitude'

    # reading application data
    app = Application()
    settings = app.settings

    use_smom = True
    use_smom = False

    comment="Average of time-domain P moment magnitudes"
    if use_smom:
        comment="Average of frequency-domain P moment magnitudes"

    #cat = read_events('event_2.xml')
    cat = read_events('event_1.xml')
    event = cat[0]
    ev_loc = event.preferred_origin().loc

    vp_grid, vs_grid = app.get_velocities()
    vp = vp_grid.interpolate(ev_loc)[0]
    vs = vs_grid.interpolate(ev_loc)[0]

    inventory = app.get_inventory()

    for i,event in enumerate(cat):
        Mw_P, station_mags_P = calc_magnitudes_from_lambda(cat, inventory, vp=vp, vs=vs, density=2700, 
                                                           P_or_S='P', use_smom=use_smom)

        #Mw_S, station_mags_S = calc_magnitudes_from_lambda(st, event, inventories[0], vp=vp, vs=vs,
                                                            #density=2700, P_or_S='S', use_smom=use_smom)

        print("In main: Mw_P=%.1f [from disp_area]" % Mw_P)

        # Average Mw_P,Mw_S to get event Mw and wrap with list of station mags/contributions
        #Mw = 0.5 * (Mw_P + Mw_S)
        #station_mags = station_mags_P + station_mags_S
        Mw = Mw_P
        station_mags = station_mags_P
        set_new_event_mag(event, station_mags, Mw, comment)

        for mag in event.magnitudes:
            print(mag)

    cat.write("event_2.xml", format='QUAKEML')



if __name__ == '__main__':

    main()
