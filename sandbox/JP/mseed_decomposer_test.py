# this script decompose the message and send the decomposed mseed to a Kafka
# queue

from microquake.io.waveform import core
from importlib import reload
from microquake.core import Trace, Stream
import numpy as np
from pandas import DataFrame
from spp.utils.kafka import KafkaHandler
from logging import getLogger
from microquake.io import msgpack
from io import BytesIO

kafka_brokers = ['localhost:9092']
kafka_topic = 'location'

kafka_handler = KafkaHandler(kafka_brokers)

logger = getLogger(__name__)

reload(core)

trs = []
for i in np.arange(10):
    tr = Trace(data=np.random.randn(1000))
    tr.stats.network = 'TEST'
    tr.stats.station = '%d' % i
    tr.stats.channel = 'X'
    trs.append(tr)

st = Stream(traces=trs)

result = core.mseed_decomposer(st)

df = DataFrame(result)

# needs to be organized by key for the recomposer to work
df_grouped = df.groupby(['key'])

logger.debug("Grouped DF Stats:" + str(df_grouped.size()))

chunks = []
for name, group in df_grouped:
    data = b''
    for g in group['blob'].values:
        data += g
    timestamp = int(name.timestamp() * 1e3)
    key = name.strftime('%Y-%d-%m %H:%M:%S.%f').encode('utf-8')
    pfle = 'pack.dat'
    f = open(pfle, 'rb')
    buf = f.read()
    data = msgpack.unpack(buf)
    buf_out = BytesIO()
    data[1].write(buf_out, format='QUAKEML')
    data_out = msgpack.pack([buf_out.getvalue()])
    kafka_handler.send_to_kafka(topic=data_out, key=key, message=data_out)
kafka_handler.producer.flush()
