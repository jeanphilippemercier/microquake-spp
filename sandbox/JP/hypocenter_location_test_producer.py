from microquake.io import msgpack
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from obspy.core.stream import read
from obspy.core.event import read_events
from io import BytesIO
import numpy as np
from glob import glob

app = Application()
settings = app.settings

logger = app.get_logger(settings.nlloc.log_topic, settings.nlloc.log_file_name)

project_code = settings.project_code
base_folder = settings.nlloc.nll_base

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.picker.kafka_consumer_topic

kafka_handler = KafkaHandler(kafka_brokers, msg_maxsize_mb=500)

logger.info('reading the catalog and waveform')

st = read('2018-11-08T11:16:48.030167Z.mseed')
# st = read('2018-11-08T10:21:49.898496Z.mseed')
# st = read('2018-11-08T11:15:29.625845Z.mseed')
cat = read_events('test.xml')


# with open('pack.dat', 'rb') as tmp:
#     data = tmp.read()
#
# obj = msgpack.unpack(data)
#
# # st = obj[0]
# # cat = obj[1]
#
# for pk in obj[0][0].picks:
#     pk.waveform_id.station_code = str(int(pk.waveform_id.station_code))
#
# st_io = BytesIO()
# obj[1].write(st_io, format='MSEED')
# ev_io = BytesIO()
# obj[0].write(ev_io, format='QUAKEML')
for k, fle in enumerate(np.sort(glob('*.mseed'))):
# k=2
    logger.info(fle)
    st = read(fle)
    st_io = BytesIO()
    st.write(st_io, format='MSEED')
    ev_io = BytesIO()

    cat[k].write(ev_io, format='QUAKEML')

    logger.info('packaging the event and stream')

    data = msgpack.pack([ev_io.getvalue(), st_io.getvalue()])

    logger.info('Message size is %d MB' % (len(data) / 1024 ** 2))

    timestamp_ms = int(cat[k].preferred_origin().time.timestamp * 1e3)

    key = str(cat[k].preferred_origin().time).encode('utf-8')

    kafka_handler.send_to_kafka(kafka_topic, key, message=data,
                                timestamp_ms=timestamp_ms)

    kafka_handler.producer.flush()

    logger.info('finished')


# data = msgpack.unpack(buf)



