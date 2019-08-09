from time import time

import numpy as np

from loguru import logger
from microquake.core import UTCDateTime
from microquake.core.stream import Stream, Trace
from spp.core.connectors import create_postgres_session
from spp.core.db_models import Recording
from spp.core.settings import settings


def get_continuous_data(starttime, endtime):
    db_name = settings.get('postgres_db').db_name

    session = create_postgres_session(db_name=db_name)

    t0 = time()
    results = session.query(Recording).filter(Recording.time <= endtime).filter(
        Recording.end_time >= starttime).all()
    t1 = time()
    logger.info('retrieving the data in {} seconds'.format(t1 - t0))

    trs = []

    for trace in results:
        tr = Trace()

        for channel in ['x', 'y', 'z']:

            if np.all(trace.__dict__[channel] == 0):
                continue

            tr.stats.network = settings.NETWORK_CODE
            tr.stats.station = trace.sensor_id
            tr.stats.location = ''
            tr.stats.channel = channel
            tr.stats.sampling_rate = trace.sample_rate
            tr.stats.starttime = UTCDateTime(trace.time)
            tr.data = np.array(trace.__dict__[channel])
            trs.append(tr)

    st = Stream(traces=trs)
    st.trim(starttime=UTCDateTime(starttime), endtime=UTCDateTime(endtime),
            pad=False, fill_value=0)

    return st
