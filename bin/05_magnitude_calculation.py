#!/usr/bin/env python3

from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.waveform import mag
from microquake.io import msgpack
from microquake.core import read_events, read
from io import BytesIO
from time import time

def magnitude(cat=None, stream=None, extra_msgs=None, logger=None, vp=None,
              vs=None, site=None):
        from time import time

        vp = vp_grid.interpolate(cat[0].preferred_origin().loc)[0]
        vs = vs_grid.interpolate(cat[0].preferred_origin().loc)[0]

        logger.info('calculating the moment magnitude')
        t3 = time()
        cat_out = mag.moment_magnitude(stream, cat[0], site, vp, vs,
                                       ttpath=None, only_triaxial=True,
                                       density=2700, min_dist=20,
                                       win_length=0.02, len_spectrum=2 ** 14,
                                       freq=100)
        t4 = time()
        logger.info('done calculating the moment magnitude in %0.3f' %
                    (t4 - t3))

        return cat_out, stream


__module_name__ = 'magnitude'

app = Application(module_name=__module_name__)
app.init_module()
vp_grid, vs_grid = app.get_velocities()
site = app.get_stations()

app.logger.info('awaiting message from Kafka')

try:
    for msg_in in app.consumer:
        try:
            cat_out, st = app.receive_message(msg_in, magnitude, vp=vp_grid,
                                              vs=vs_grid, site=site)
        except Exception as e:
            app.logger.error(e)

        app.send_message(cat_out, st)
        app.logger.info('awaiting message from Kafka')

except KeyboardInterrupt:
    app.logger.info('received keyboard interrupt')

finally:
    app.logger.info('closing Kafka connection')
    app.consumer.close()
    app.logger.info('connection to Kafka closed')