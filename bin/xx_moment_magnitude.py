
from microquake.core.event import read_events as read_events
from microquake.waveform.mag_new import calc_magnitudes_from_lambda, set_new_event_mag
from spp.utils.application import Application

from lib_process import processCmdLine

def main():

    fname = 'moment_magnitude'

    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname)

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger('xx_moment_magnitude', 'zlog')
    vp_grid, vs_grid = app.get_velocities()

    density = settings.magnitude.density

    if use_web_api:
        api_base_url = settings.seismic_api.base_url
        request = get_event_by_id(api_base_url, event_id)
        if request is None:
            logger.error("seismic api returned None!")
            exit(0)
        cat = request.get_event()

    else:
        cat  = read_events(xml_in)


    for i,event in enumerate(cat):

        ev_loc = event.preferred_origin().loc
        vp = vp_grid.interpolate(ev_loc)[0]
        vs = vs_grid.interpolate(ev_loc)[0]


        Mw_P, station_mags_P = calc_magnitudes_from_lambda([event],
                                                           vp=vp,
                                                           vs=vs,
                                                           density=density,
                                                           P_or_S='P',
                                                           use_smom=False,
                                                           use_sdr=False,
                                                           use_free_surface_correction=False,
                                                           sdr=(0,80,-90),
                                                           logger=logger)

        print("In main: Mw_P=%.1f [from disp area]" % Mw_P)

        comment="Average of time-domain P station moment magnitudes"
        Mw = Mw_P
        station_mags = station_mags_P
        set_new_event_mag(event, station_mags, Mw, comment, make_preferred=True)


    cat.write(xml_out, format='QUAKEML')

    return


if __name__ == '__main__':

    main()