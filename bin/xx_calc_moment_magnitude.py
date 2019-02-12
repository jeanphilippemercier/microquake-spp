
from microquake.core.event import read_events as read_events
from microquake.waveform.mag_new import calc_magnitudes_from_lambda, set_new_event_mag
from spp.utils.application import Application

from lib_process import processCmdLine

from logging import getLogger
logger = getLogger()

def main():

    fname = 'calc_moment_magnitude'

    use_web_api, xml_out, xml_in, mseed_in = processCmdLine(fname)

    if use_web_api:
        logger.info("Read from web_api")
        api_base_url = settings.seismic_api.base_url
        start_time = UTCDateTime("2018-07-06T11:21:00")
        end_time = start_time + 3600.
        request = get_events_catalog(api_base_url, start_time, end_time)
        cat = request[0].get_event()
    else:
        logger.info("Read from files on disk")
        cat  = read_events(xml_in)


    # reading application data
    app = Application()
    settings = app.settings
    vp_grid, vs_grid = app.get_velocities()

    use_smom = True
    use_smom = False

    comment="Average of time-domain P moment magnitudes"
    if use_smom:
        comment="Average of frequency-domain P moment magnitudes"


    for i,event in enumerate(cat):

        ev_loc = event.preferred_origin().loc
        vp = vp_grid.interpolate(ev_loc)[0]
        vs = vs_grid.interpolate(ev_loc)[0]

        Mw_P, station_mags_P = calc_magnitudes_from_lambda(cat, vp=vp, vs=vs, density=2700, 
                                                           P_or_S='P', use_smom=use_smom)

        #print("In main: Mw_P=%.1f [from disp_area]" % Mw_P)

        #Mw_S, station_mags_S = calc_magnitudes_from_lambda(cat, vp=vp, vs=vs, density=2700, 
                                                           #P_or_S='S', use_smom=use_smom)
        #print("In main: Mw_S=%.1f [from disp_area]" % Mw_S)


        # Average Mw_P,Mw_S to get event Mw and wrap with list of station mags/contributions
        #Mw = 0.5 * (Mw_P + Mw_S)
        #station_mags = station_mags_P + station_mags_S

        Mw = Mw_P
        station_mags = station_mags_P
        set_new_event_mag(event, station_mags, Mw, comment)


    cat.write(xml_out, format='QUAKEML')



if __name__ == '__main__':

    main()
