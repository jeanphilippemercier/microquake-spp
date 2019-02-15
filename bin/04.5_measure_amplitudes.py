#!/usr/bin/env python3

import numpy as np
import sys

from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.core.stream import Stream
from microquake.waveform.amp_measures import measure_pick_amps
from microquake.waveform.transforms import rotate_to_ENZ, rotate_to_P_SV_SH

from spp.utils.application import Application


def measure_amplitudes(cat=None, stream=None, extra_msgs=None, logger=None, params=None, app=None):

    st = stream.copy()
    cat_out = cat.copy()

    inventory = app.get_inventory()
    st.attach_response(inventory)

    sta_meta_dict = inv_station_list_to_dict(inventory)

# 1. Rotate traces to ENZ
    st_rot = rotate_to_ENZ(st, sta_meta_dict)
    st = st_rot

# 2. Rotate traces to P,SV,SH wrt event location
    st_new = rotate_to_P_SV_SH(st, cat_out)
    st = st_new

# 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
    trP = [tr for tr in st if tr.stats.channel == 'P']
    measure_pick_amps(Stream(traces=trP), cat_out, phase_list=['P'], \
                      min_pulse_width=.00167, min_pulse_snr=5, debug=False)

    return cat_out, stream


__module_name__ = 'measure_amplitudes'

def main(argv):

    app = Application(module_name=__module_name__)
    app.init_module()

    # reading application data
    settings = app.settings

    # This is wrong --> will fix later
    params = app.settings.picker
    logger = app.get_logger(settings.create_event.log_topic,
                            settings.create_event.log_file_name)

    app.logger.info('awaiting message from Kafka')
    try:
        for msg_in in app.consumer:
            try:
                cat, st = app.receive_message(msg_in, measure_amplitudes, params=params, app=app)
                cat.write('test_measure_amps.xml', format='QUAKEML')

            except Exception as e:
                logger.error(e)

            app.send_message(cat, st)
            app.logger.info('awaiting message from Kafka')

            logger.info('awaiting Kafka messsages')

    except KeyboardInterrupt:
        logger.info('received keyboard interrupt')

    finally:
        logger.info('closing Kafka connection')
        app.consumer.close()
        logger.info('connection to Kafka closed')

    return

if __name__ == "main":
    main(sys.argv[1:])
