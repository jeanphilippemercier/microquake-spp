#!/usr/bin/env python3

from spp.utils.application import Application
from microquake.nlloc import NLL, calculate_uncertainty


def location(cat=None, stream=None, extra_msgs=None, logger=None, nll=None,
             params=None, project_code=None):

    from time import time
    from IPython.core.debugger import Tracer

    logger.info('unpacking the data received from Kafka topic <%s>'
                % settings.nlloc.kafka_consumer_topic)

    logger.info('running NonLinLoc')
    t0 = time()
    cat_out = nll.run_event(cat[0].copy())
    t1 = time()
    logger.info('done running NonLinLoc in %0.3f seconds' % (t1 - t0))

    base_folder = params.nll_base

    logger.info('calculating Uncertainty')
    t2 = time()
    origin_uncertainty = calculate_uncertainty(cat_out[0], base_folder,
                                               project_code,
                                               perturbation=5,
                                               pick_uncertainty=1e-3)

    cat_out[0].preferred_origin().origin_uncertainty = origin_uncertainty
    t3 = time()
    logger.info('done calculating uncertainty in %0.3f seconds' % (t3 - t2))

    return cat_out, stream

__module_name__ = 'nlloc'

app = Application(module_name=__module_name__)
app.init_module()

# reading application data
settings = app.settings

project_code = settings.project_code
base_folder = settings.nlloc.nll_base
gridpar = app.nll_velgrids()
sensors = app.nll_sensors()
params = app.settings.nlloc

# Preparing NonLinLoc
app.logger.info('preparing NonLinLoc')
nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
          sensors=sensors, params=params)
app.logger.info('done preparing NonLinLoc')


app.logger.info('awaiting message from Kafka')
while True:
    msg_in = app.consumer.poll(timeout=1)
    if msg_in is None:
        continue
    if msg_in.value() == b'Broker: No more messages':
        continue
    cat_out, st = app.receive_message(msg_in, location, nll=nll, params=params,
                                      project_code=project_code)

    app.send_message(cat_out, st)
    app.logger.info('awaiting message from Kafka')
