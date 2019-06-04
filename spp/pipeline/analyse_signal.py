import numpy as np

from loguru import logger
from microquake.core import UTCDateTime


class Process():
    def __init__(self, app, module_settings):
        self.app = app
        self.module_settings = module_settings

    def process(
        self,
        cat=None,
        stream=None,
    ):
        signal_quality_data = []

        try:
            for trace in stream.composite():
                sta = trace.stats.station
                logger.info('Analysing signal for station {}'.format(sta))
                start_time = UTCDateTime(trace.stats.starttime)
                end_time = UTCDateTime(trace.stats.endtime)
                amplitude = np.std(trace.data)
                dt = end_time - start_time
                nsamp = dt * trace.stats.sampling_rate
                data = np.nan_to_num(trace.data)
                non_missing_ratio = len(np.nonzero(data != 0)[0]) / nsamp
                energy = amplitude * 1e6
                integrity = non_missing_ratio
                signal_quality_data.append({
                    'station_code': sta,
                    'energy': energy,
                    'integrity': integrity,
                    'sampling_rate': trace.stats.sampling_rate,
                    'num_samples': nsamp,
                    'amplitude': amplitude,
                })
                logger.info('Done analysing signal for station %s, energy: %0.3f, integrity: %0.2f' % (sta, amplitude * 1e6,
                                                                                                       non_missing_ratio))

        except Exception as e:
            logger.error(e)

        return signal_quality_data


__module_name__ = 'signal_analysis'
