#!/usr/bin/env python3

from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.waveform.mag_new import calc_magnitudes_from_lambda, set_new_event_mag

from spp.utils.application import Application

def magnitude(cat=None, stream=None, extra_msgs=None, logger=None,
             params=None, vp_grid=None, vs_grid=None, use_smom=False):

    cat_out = cat.copy()

    comment="Average of time-domain P moment magnitudes"
    if use_smom:
        comment="Average of frequency-domain P moment magnitudes"


    for i,event in enumerate(cat_out):

        ev_loc = event.preferred_origin().loc
        vp = vp_grid.interpolate(ev_loc)[0]
        vs = vs_grid.interpolate(ev_loc)[0]

        Mw_P, station_mags_P = calc_magnitudes_from_lambda(cat_out, vp=vp, vs=vs, density=2700,.
                                                           P_or_S='P', use_smom=use_smom)
        Mw = Mw_P
        station_mags = station_mags_P
        set_new_event_mag(event, station_mags, Mw, comment)


    return cat_out, stream



__module_name__ = 'magnitude'

def main(argv):

    app = Application(module_name=__module_name__)
    app.init_module()

    # reading application data
    #settings = app.settings

    vp_grid, vs_grid = app.get_velocities()


    try:
        for msg_in in app.consumer:

            try:
                cat_out, st = app.receive_message(msg_in, magnitude, vp_grid=vp_grid,
                                                  vs_grid=vs_grid, use_smom=False)

                cat_out.write('test_mag.xml', format='QUAKEML')

            except Exception as e:
                app.logger.error(e)
                continue

            app.send_message(cat_out, st)

    except KeyboardInterrupt:
        app.logger.info('received keyboard interrupt')

    finally:
        app.logger.info('closing Kafka connection')
        app.consumer.close()
        app.logger.info('connection to Kafka closed')

    return

if __name__ == "__main__":
    main(sys.argv[1:])
