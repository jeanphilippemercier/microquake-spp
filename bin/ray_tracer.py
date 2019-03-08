import os
from datetime import datetime
from time import time
from spp.utils.grid import Grid
from spp.utils.cli import CLI

import numpy as np

from spp.utils.application import Application

# app = Application(module_name='ray_tracer')
# inventory = app.get_inventory()

def process(
    cat=None,
    stream=None,
    logger=None,
    app=None,
    module_settings=None,
    prepared_objects=None,):

    inventory = app.get_inventory()
    gd = Grid()

    for phase in ['P', 'S']:
        for event in cat:
            for station in inventory.stations():
                logger.info('calculating ray for station %s and location %s'
                            % (station.code, event.loc))
                ray = gd.get_ray(station.code, phase,
                                 event.preferred_origin().loc)

                # Next, send the data back to the api

def main():
    __module_name__ = "ray_tracer"
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()