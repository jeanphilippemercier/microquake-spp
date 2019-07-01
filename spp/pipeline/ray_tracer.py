from loguru import logger
from spp.utils import seismic_client
from spp.utils.grid import Grid

from ..core.settings import settings
from .processing_unit import ProcessingUnit
from microquake.core import read_events
from io import BytesIO


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

        cat = kwargs["cat"]

        gd = Grid()

        event_id = str(cat[0].resource_id)
        logger.info("pipeline: ray_tracer - start processing rays for event_id: %s" % event_id)

        self.result = []

        for phase in ['P', 'S']:
            for origin in cat[0].origins:
                origin_id = str(origin.resource_id)

                for station in settings.inventory.stations():
                    logger.info('calculating ray [event_id: %s, origin_id: %s] for station %s and location %s'
                                % (event_id, origin_id, station.code, origin.loc))
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
                        if not arrival:
                            continue
                        pick = arrival.get_pick()

                        if not pick:
                            continue

                        if pick.waveform_id.station_code == station.code:
                            if arrival.phase == phase:
                                arrival_id = str(arrival.resource_id)

                    # post ray data to api
                    result = {'event_id': event_id,
                              'origin_id': origin_id,
                              'arrival_id': arrival_id,
                              'station_id': station_id,
                              'phase': phase,
                              'travel_time': travel_time,
                              'azimuth': azimuth,
                              'toa': toa,
                              'ray_nodes': ray.nodes}

                    seismic_client.post_ray(self.api_url,
                                            self.site_code,
                                            self.network_code,
                                            event_id,
                                            origin_id,
                                            arrival_id,
                                            station_id,
                                            phase,
                                            travel_time,
                                            azimuth,
                                            toa,
                                            ray.nodes)

                    self.result.append(result)

        return self.result

    def legacy_pipeline_handler(
        self,
        msg_in,
        res
    ):
        _, stream = self.app.deserialise_message(msg_in)

        return res['cat'], stream


def ray_tracer_pipeline(event_bytes=None):

    ray_tracer_processor = Processor()
    ray_tracer_processor.initializer()

    cat = read_events(BytesIO(event_bytes), format='quakeml')

    result = ray_tracer_processor.process(cat=cat)

    # post the result to the API

    logger.info('done calculating rays for event %s'
                % cat[0].origins[-1].time)
