from microquake.io import msgpack
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from microquake.core import read_events
from io import BytesIO

app = Application()
settings = app.settings
logger = app.get_logger()

project_code = settings.project_code
base_folder = settings.nlloc.nll_base

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.nlloc.kafka_consumer_topic

kafka_handler = KafkaHandler(kafka_brokers)

logger.info('reading the catalog and waveform')

st = read('2018-11-08T11:16:48.030167Z.mseed')
cat = read_events('test.xml')

st_io = BytesIO()
st.write(st_io, format='MSEED')
ev_io = BytesIO()
cat[2].write(ev_io, format='QUAKEML')

logger.info('packaging the event and stream')

with open('pack.dat', 'rb') as tmp:
    data = tmp.read()

obj = msgpack.unpack(data)

# st = obj[0]
# cat = obj[1]

for pk in obj[0][0].picks:
    pk.waveform_id.station_code = str(int(pk.waveform_id.station_code))

st_io = BytesIO()
obj[1].write(st_io, format='MSEED')
ev_io = BytesIO()
obj[0].write(ev_io, format='QUAKEML')

data = msgpack.pack([ev_io.getvalue(), st_io.getvalue()])

timestamp = cat[2].preferred_origin().time.timestamp * 1e3

key = str(cat[2].preferred_origin().time).encode('utf-8')


kafka_handler.send_to_kafka(kafka_topic, message=data,
                            key=key, timestamp=int(timestamp))

kafka_handler.producer.flush()


# data = msgpack.unpack(buf)



