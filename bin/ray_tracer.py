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
        for origin in cat[0].origins:
            for station in inventory.stations():
                logger.info('calculating ray for station %s and location %s'
                            % (station.code, origin.loc))
                ray = gd.get_ray(station.code, phase,
                                 origin.loc)
                tt = gd.get_grid_point(station.code, phase, origin.loc,
                                       type='time')


                # 1) Write the post endpoint
                # 2) Get the pod running
                # 3) Post an event and send the event to the Kafka topic
                # ray_tracer
                # 4) write a function in seismic_client to post the data to
                # the api
                # 5) write the get endpoint
                # 6) write a function in seismic_client to get the data from
                #  the API.

                # Next, send the data back to the api
                # site id, event_id, origin_id, station_id, arrival_id (can be
                # NULL),length, travel_time, nodes (an array of (x, y,
                # z) encoded as bytes or base64)
    return

__module_name__ = "ray_tracer"

def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()