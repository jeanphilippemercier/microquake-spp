from spp.utils import seismic_client
from spp.utils.cli import CLI
from spp.utils.grid import Grid


def process(
        cat=None,
        stream=None,
        logger=None,
        app=None,
        module_settings=None,
        prepared_objects=None, ):

    inventory = app.get_inventory()
    gd = Grid()
    site_code = app.settings.site_code
    network_code = app.settings.network_code

    event_id = str(cat[0].resource_id)
    for phase in ['P', 'S']:
        for origin in cat[0].origins:
            origin_id = str(origin.resource_id)
            for station in inventory.stations():
                logger.info('calculating ray for station %s and location %s'
                            % (station.code, origin.loc))
                ray = gd.get_ray(station.code, phase,
                                 origin.loc)
                travel_time = gd.get_grid_point(station.code, phase,
                                                origin.loc, type='time')
                azimuth = gd.get_grid_point(station.code, phase, origin.loc,
                                            type='azimuth')
                toa = gd.get_grid_point(station.code, phase, origin.loc,
                                            type='take_off')
                station_id = station.code

                arrival_id = None
                for arrival in origin.arrivals:
                    pick = arrival.get_pick()
                    if pick.waveform_id.station_code == station.code:
                        if arrival.phase == phase:
                            arrival_id = str(arrival.resource_id)

                # post ray data to api
                seismic_client.post_ray(app.settings.seismic_api.base_url,
                                        site_code, network_code, event_id,
                                        origin_id, arrival_id, station_id,
                                        phase, ray.length(), travel_time,
                                        azimuth, toa, ray.nodes)

    return cat, stream


__module_name__ = "ray_tracer"


def main():
    cli = CLI(__module_name__, processing_flow_name='ray_tracing',
              callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
