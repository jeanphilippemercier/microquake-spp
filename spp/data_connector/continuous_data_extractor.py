from microquake.clients.ims import web_client
from microquake.core.settings import settings
from microquake.db.connectors import (RedisQueue, write_trigger)
from microquake.db.serializers.continuous_data import (write_continuous_data,
                                                write_processing_record,
                                    continuous_get_not_processed_sensors)
from microquake.helpers.time import get_time_zone
from obspy.core import UTCDateTime
from microquake.helpers.logging import logger
from microquake.processors import event_detection
import numpy as np
from uuid import uuid4

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

        write_trigger(trigger_on.datetime,
                      trigger_off.datetime,
                      amplitude,
                      sensor_id,
                      trigger_band_id)

    return


def extract_continuous_triggering(sensor_code, start_time,
                                  minimum_delay_second=2,
                                  taper_length_second=0.01,
                                  max_length_minute=2):

    write_processing_record(sensor_code, ttl_minutes=5)

    max_length_second = max_length_minute * 60

    end_time = UTCDateTime.now()

    if end_time - start_time > max_length_second:
        start_time = end_time - max_length_second

    new_job_id = f'{uuid4()}_{sensor_code}'

    end_time = UTCDateTime.now()

    logger.info(f'extracting continuous data for sensor {sensor_code} between'
                f' {start_time} and {end_time}')

    if end_time - start_time < minimum_delay_second:
        logger.info(f'start and end times are within {minimum_delay_second} '
                    f'second from each other... skipping')
        logger.info(f'submitting task {new_job_id} on queue{job_queue.queue}')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time, job_id=new_job_id)
        return
    try:
        cd = web_client.get_continuous(ims_base_url, start_time, end_time,
                                       [int(sensor_code)])
    except Exception as e:
        logger.error(e)
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time, job_id=new_job_id)

    if len(cd) == 0:
        logger.warning(f'no continuous data extracted for sensor '
                       f'{sensor_code}')
        logger.info(f'submitting task {new_job_id} on queue{job_queue.queue}')

        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time, job_id=new_job_id)

        return
    if len(cd[0].data) < taper_length_second * cd[0].stats.sampling_rate:
        logger.warning(f'The trace is too short '
                       f'{sensor_code}')
        logger.info(f'submitting task {new_job_id} on queue{job_queue.queue}')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time, job_id=new_job_id)

        return

    try:

        trace_start_time = cd[0].stats.starttime
        trace_end_time = cd[0].stats.endtime

        segment_length = trace_end_time - trace_start_time
        if trace_end_time - trace_start_time < 2:
            logger.info(f'the trace is too small...skipping')
            logger.info(f'submitting task {new_job_id} on '
                        f'queue{job_queue.queue}')
            job_queue.submit_task(extract_continuous_triggering, sensor_code,
                                  start_time, job_id=new_job_id)

        new_start_time = trace_end_time - 2 * taper_length_second

        logger.info(f'segment length {segment_length}')
        logger.info(f'trace start time: {trace_start_time}')
        logger.info(f'trace end time: {trace_end_time}')

        logger.info('writing continuous data')
        write_continuous_data(cd, ttl_hour=6)
        logger.info('done writing continuous data')

        logger.info(f'submitting task {new_job_id} on queue{job_queue.queue}')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              new_start_time, job_id=new_job_id)

    except Exception as e:
        logger.error(e)
        logger.info(f'submitting task {new_job_id} on queue{job_queue.queue}')
        job_queue.submit_task(extract_continuous_triggering, sensor_code,
                              start_time, job_id=new_job_id)

    logger.info(f'performing triggering for sensor {sensor_code}')

    try:
        tmp_trace = cd.copy().composite()
        logger.info(f'len(tmp_trace) = {len(tmp_trace)}')
        logger.info(f'the length of the trace is {len(tmp_trace[0].data)}')
        tmp_trace = tmp_trace.detrend('demean').detrend('linear')
        tmp_trace = tmp_trace.taper(max_percentage=0.01,
                                    max_length=taper_length_second,
                                    side='both')
        triggering(tmp_trace[0])
    except Exception as e:
        logger.error(e)

    return


if __name__ == '__main__':

    from spp.data_connector.continuous_data_extractor import \
        extract_continuous_triggering as ect

    from importlib import reload

    s_time = UTCDateTime.now() - 60

    for sensor in continuous_get_not_processed_sensors():
        job_id = f'continuous_record:{uuid4()}'
        sc = sensor
        job_queue.submit_task(ect, sc, s_time, job_id=job_id,
                              minimum_delay_second=2)


