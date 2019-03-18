
import numpy as  np
from microquake.core.event import read_events as read_events
from microquake.waveform.mag_new import calc_magnitudes_from_lambda, set_new_event_mag
from spp.utils.application import Application

from lib_process import processCmdLine

def main():

    fname = 'xx_moment_magnitude'

    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname)

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger(fname, 'zlog')
    vp_grid, vs_grid = app.get_velocities()

    params = settings.magnitude
    density = params.density
    min_dist = params.min_dist
    use_sdr_rad = params.use_sdr_rad
    use_free_surface_correction = params.use_free_surface_correction
    make_preferred = params.make_preferred
    phase_list = params.phase_list
    if not isinstance(phase_list, list):
        phase_list = [phase_list]

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

        sdr = None
        if use_sdr_rad:
            focal_mech = event.preferred_focal_mechanism()
            if focal_mech is not None:
                nodal_plane = focal_mech.nodal_planes.nodal_plane_1
                strike = nodal_plane.strike
                dip = nodal_plane.dip
                rake = nodal_plane.rake
                sdr = (strike, dip, rake)
                logger.info("%s: use_sdr_rad=True (s,d,r)=(%.1f,%.1f,%.1f)" % \
                            (fname, strike, dip, rake))

        Mws = []
        station_mags = []

        print("xx_moment_mag: id(logger)=%d" % id(logger))
        for phase in phase_list:

            Mw, sta_mags = calc_magnitudes_from_lambda(
                                [event],
                                vp=vp,
                                vs=vs,
                                density=density,
                                P_or_S=phase,
                                use_smom=False,
                                use_sdr_rad=use_sdr_rad,
                                use_free_surface_correction=use_free_surface_correction,
                                sdr=sdr,
                                min_dist=min_dist,
                                logger_in=logger)

            Mws.append(Mw)
            station_mags.extend(sta_mags)
            print("In main: Mw_%s=%.1f [from disp area]" % (phase, Mws[-1]))

        Mw = np.mean(Mws)
        comment="Average of time-domain P station moment magnitudes"
        if use_sdr_rad and sdr is not None:
            comment += " Use_sdr_rad: sdr=(%.1f,%.1f,%.1f)" % (sdr[0],sdr[1],sdr[2])

        set_new_event_mag(event, station_mags, Mw, comment, make_preferred=make_preferred)

    cat.write(xml_out, format='QUAKEML')

    return


if __name__ == '__main__':

    main()
