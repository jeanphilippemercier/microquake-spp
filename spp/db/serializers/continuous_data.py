from datetime import datetime, timedelta
from time import time

from obspy.core import UTCDateTime

from loguru import logger
from microquake.core.stream import Stream, Trace
from spp.db.models.alchemy import (ContinuousData)
from spp.db.models.redis import set_event, get_event
from spp.db.connectors import (write_document_postgres,
                               create_postgres_session)

from uuid import uuid4


def write_continuous_data(continuous_data, ttl_hour=6):

    redis_key = str(uuid4())

    ttl_second = ttl_hour * 3600  # 6 hours
    set_event(redis_key, fixed_length=continuous_data, ttl=ttl_second)

    start_time = continuous_data[0].stats.starttime
    end_time = continuous_data[0].stats.endtime
    sensor_id = continuous_data[0].stats.station

    expiry_time = datetime.utcnow() + timedelta(hours=ttl_hour)

    document = {'start_time': start_time,
                'end_time': end_time,
                'sensor_id': sensor_id,
                'expiry_time': expiry_time,
                'redis_key': redis_key}

    return write_document_postgres(document)


def get_continuous_data(start_time, end_time, sensor_id=None):

    session, engine = create_postgres_session()

    t0 = time()

    if sensor_id is not None:
        session.query(ContinuousData).filter(
            ContinuousData.start_time <= end_time).filter(
            ContinuousData.end_time >= start_time).filter(
            ContinuousData.sensor_id == sensor_id).all()

    else:
        results = session.query(ContinuousData).filter(
            ContinuousData.start_time <= end_time).filter(
            ContinuousData.end_time >= start_time).all()

    t1 = time()
    logger.info('retrieving the data in {} seconds'.format(t1 - t0))

    trs = []

    for trace in results:

        event = get_event(trace.redis_key)

        st = event['fixed_length']

        for tr in st:
            trs.append(tr)

        # for channel in ['x', 'y', 'z']:
        #
        #     if np.all(trace.__dict__[channel] == 0):
        #         continue
        #
        #     tr.stats.network = settings.NETWORK_CODE
        #     tr.stats.station = str(trace.sensor_id)
        #     tr.stats.location = ''
        #     tr.stats.channel = channel
        #     tr.stats.sampling_rate = trace.sample_rate
        #     tr.stats.starttime = UTCDateTime(trace.time)
        #     tr.data = np.array(trace.__dict__[channel])
        #     trs.append(tr)

    stream = Stream(traces=trs)
    stream.trim(starttime=UTCDateTime(start_time),
                endtime=UTCDateTime(end_time),
                pad=False, fill_value=0)

    session.close()
    engine.dispose()

    return st