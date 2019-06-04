from loguru import logger
from spp.utils import seismic_client
from spp.utils.grid import Grid

from ..core.settings import settings


class Processor():
    def __init__(self, app, module_settings):
        self.module_settings = module_settings
        self.site_code = settings.SITE_CODE
        self.network_code = settings.NETWORK_CODE

    def process(
        self,
        cat=None,
        stream=None,
    ):
        gd = Grid()

        event_id = str(cat[0].resource_id)

        for phase in ['P', 'S']:
            for origin in cat[0].origins:
                origin_id = str(origin.resource_id)

                for station in settings.inventory.stations():
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
                    seismic_client.post_ray(settings.get('seismic_api').base_url,
                                            self.site_code, self.network_code, event_id,
                                            origin_id, arrival_id, station_id,
                                            phase, ray.length(), travel_time,
                                            azimuth, toa, ray.nodes)

        return cat, stream
