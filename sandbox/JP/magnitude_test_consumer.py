from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.waveform import mag
from microquake.io import msgpack
from obspy.core.stream import read
from obspy.core.event import read_events, read
from io import BytesIO
from IPython.core.debugger import Tracer

from importlib import reload
reload(mag)

app = Application()
settings = app.settings
logger = app.get_logger()
site = app.get_stations()
vp_grid, vs_grid = app.get_velocities()


kafka_brokers = settings.kafka.brokers
kafka_topic = settings.magnitude.kafka_consumer_topic
kafka_producer_topic = settings.magnitude.kafka_producer_topic

kafka_handler = KafkaHandler(kafka_brokers)
consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

logger.info("Awaiting Kafka mseed messsages")
for msg_in in consumer:
    data = msgpack.unpack(msg_in.value)
    st = read(BytesIO(data[1]))
    cat = read_events(BytesIO(data[0]))

    # cat_out = moment_magnitude(stream, cat[0], site, vp, vs,
    #                            **settings.magnitude)

    vp = vp_grid.interpolate(cat[0].preferred_origin().loc)[0]
    vs = vs_grid.interpolate(cat[0].preferred_origin().loc)[0]

    cat_out = mag.moment_magnitude(st, cat[0], site, vp, vs, ttpath=None,
    only_triaxial=True, density=2700, min_dist=20, win_length=0.02,
                               len_spectrum=2 ** 14, freq=100)

    Tracer()()
