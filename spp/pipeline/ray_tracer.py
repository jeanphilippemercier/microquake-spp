from loguru import logger
from spp.utils import seismic_client
from spp.utils.grid import Grid

from ..core.settings import settings
from .processing_unit import ProcessingUnit


class Processor(ProcessingUnit):
    @property
    def module_name(self):
        return "ray_tracer"

    def initializer(self):
        self.site_code = settings.SITE_CODE
        self.network_code = settings.NETWORK_CODE
        self.api_url = settings.API_BASE_URL

    def process(
        self,
        **kwargs
    ):
        logger.info("pipeline: raytracer")

        cat = kwargs["cat"]

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
                    seismic_client.post_ray(self.api_base_url,
                                            self.site_code, self.network_code, event_id,
                                            origin_id, arrival_id, station_id,
                                            phase, ray.length(), travel_time,
                                            azimuth, toa, ray.nodes)

        return {'cat': cat}

    def legacy_pipeline_handler(
        self,
        msg_in,
        res
    ):
        _, stream = self.app.deserialise_message(msg_in)

        return res['cat'], stream
