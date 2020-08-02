from spp.clients.ims import web_client
from microquake.core.settings import settings
from microquake.db.connectors import (RedisQueue, connect_redis)
from microquake.core.helpers.time import get_time_zone
from obspy.core import UTCDateTime
from loguru import logger
from uuid import uuid4
from microquake.db.models.redis import set_event, get_event

inventory = settings.inventory
ims_base_url = settings.get('ims_base_url')
time_zone = get_time_zone()

queue = 'test'
job_queue = RedisQueue(queue)

redis_connection = connect_redis()


def extract_continuous(sensor_code, start_time):
    end_time = UTCDateTime.now()
    cd = web_client.get_continuous(ims_base_url, start_time, end_time,
                                   sensor_code, time_zone)

    if len(cd[0]) == 0:
        logger.warning(f'no continuous data extracted for sensor '
                       f'{sensor_code}')
        job_queue.submit_task(extract_continuous, sensor_code, start_time)

        return
    try:
        key = str(uuid4())

        ttl = 6 * 3600  # 6 hours
        set_event(key, fixed_length=cd, ttl=ttl)

        trace_start_time = cd[0].stats.starttime
        trace_end_time = cd[0].stats.endtime

        segment_length = trace_end_time - trace_start_time

        new_start_time = trace_end_time
        new_end_time = UTCDateTime.now()

        logger.info(f'segment length {segment_length}')
        logger.info(f'trace start time: {trace_start_time}')
        logger.info(f'trace end time: {trace_end_time}')

        job_queue.submit_task(extract_continuous, sensor_code, new_start_time)
    except Exception as e:
        logger.error(e)
        job_queue.submit_task(extract_continuous, sensor_code, start_time)

    return


if __name__ == '__main__':

    from spp.data_connector.continuous_data_extractor import \
        extract_continuous as ec

    s_time = UTCDateTime.now() - 300

    for sensor in inventory.stations():
        sc = sensor.code
        job_queue.submit_task(ec, sc, s_time)


