from spp.clients.ims import web_client
from microquake.core.settings import settings
from spp.db.connectors import (RedisQueue, write_trigger)
from spp.db.serializers.continuous_data import write_continuous_data
from microquake.core.helpers.time import get_time_zone
from obspy.core import UTCDateTime
from loguru import logger
from microquake.processors import event_detection
import numpy as np

inventory = settings.inventory
ims_base_url = settings.get('ims_base_url')
time_zone = get_time_zone()

queue = 'test'
job_queue = RedisQueue(queue)

evp = event_detection.Processor()


def triggering(trace, trigger_band_id='microseismic'):
    triggers_on, triggers_off = evp.process(trace=trace)

    if not triggers_on:
        logger.info('no trigger')
        return

    sensor_id = trace.stats.station

    triggers_out = []
    for trigger_on, trigger_off in zip(triggers_on, triggers_off):
        triggers_out.append([sensor_id, trigger_on, trigger_off])

        logger.info(len(trace))
        tr = trace.copy().trim(starttime=trigger_on - 0.01,
                               endtime=trigger_on + 0.01).data
        logger.info(len(tr))
        amplitude = np.max(np.abs(tr))

        logger.info(f'writing trigger: time={trigger_on}, '
                    f'sensor_id={sensor_id}, amplitude={amplitude}, '
                    f'trigger_band_id={trigger_band_id}')

        write_trigger(trigger_on, trigger_off, amplitude, sensor_id,
                      trigger_band_id)

    return


def extract_continuous_triggering(sensor_code, start_time,
                                  minimum_delay_second=120,
                                  taper_length_second=0.01):
    end_time = UTCDateTime.now()

    logger.info(f'extracting continuous data for sensor {sensor_code} between'
                f'{start_time} and {end_time}')

    if end_time - start_time < minimum_delay_second:
        logger(f'start and end times are within {minimum_delay_second} second '
               f'from each other... skipping')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time)
        return

    cd = web_client.get_continuous(ims_base_url, start_time, end_time,
                                   sensor_code, time_zone)

    if len(cd) == 0:
        logger.warning(f'no continuous data extracted for sensor '
                       f'{sensor_code}')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time)

        return
    if len(cd[0].data) < taper_length_second * cd[0].stats.sampling_rate:
        logger.warning(f'The trace is too short '
                       f'{sensor_code}')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time)

        return

    try:

        trace_start_time = cd[0].stats.starttime
        trace_end_time = cd[0].stats.endtime

        segment_length = trace_end_time - trace_start_time
        if trace_end_time - trace_start_time < 2:
            logger.info(f'the trace is too small...skipping')
            job_queue.submit_task(extract_continuous_triggering, sensor_code,
                                  start_time)

        new_start_time = trace_end_time - 2 * taper_length_second

        logger.info(f'segment length {segment_length}')
        logger.info(f'trace start time: {trace_start_time}')
        logger.info(f'trace end time: {trace_end_time}')

        logger.info('writing continuous data')
        write_continuous_data(cd, ttl_hour=6)
        logger.info('done writing continuous data')

        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              new_start_time)

    except Exception as e:
        logger.error(e)
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time)

    logger.info(f'performing triggering for sensor {sensor_code}')

    tmp_trace = cd.copy().composite()
    logger.info(f'len(tmp_trace) = {len(tmp_trace)}')
    logger.info(f'the length of the trace is {len(tmp_trace[0].data)}')
    tmp_trace = tmp_trace.detrend('demean').detrend('linear')
    tmp_trace = tmp_trace.taper(max_percentage=0.01,
                                max_length=taper_length_second,
                                side='both')

    triggering(tmp_trace[0])

    return


if __name__ == '__main__':

    from spp.data_connector.continuous_data_extractor import \
        extract_continuous_triggering as ect

    from importlib import reload

    s_time = UTCDateTime.now() - 300

    for sensor in inventory.stations():
        sc = sensor.code
        job_queue.submit_task(ect, sc, s_time)
        # extract_continuous_triggering(sc, s_time)


