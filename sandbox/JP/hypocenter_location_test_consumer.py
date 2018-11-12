from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.nlloc import NLL
from microquake.io import msgpack
from microquake.core import read_events, read
from io import BytesIO
from IPython.core.debugger import Tracer

app = Application()
settings = app.settings
logger = app.get_logger()

# initialize NLL object

# check if velocity is current
# if not download from server
# and run nll_prepare

project_code = settings.project_code
base_folder = settings.nlloc.nll_base
gridpar = app.nll_velgrids()
sensors = app.nll_sensors()
params = app.settings.nlloc

nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
          sensors=sensors, params=params)

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.nlloc.kafka_consumer_topic
kafka_producer_topic = settings.nlloc.kafka_producer_topic

kafka_handler = KafkaHandler(kafka_brokers)
consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

logger.info("Awaiting Kafka mseed messsages")
for msg_in in consumer:
    # logger.info("Received message with key:", msg_in.key)
    data = msgpack.unpack(msg_in.value)
    st = read(BytesIO(data[1]))
    cat = read_events(BytesIO(data[0]))
    cat_out = nll.run_event(cat[0].copy())

    timestamp = cat_out[0].preferred_origin().time.timestamp * 1e3
    key = str(cat_out[0].preferred_origin().time).encode('utf-8')

    ev_io = BytesIO()
    cat_out.write(ev_io, format='QUAKEML')
    data_out = msgpack.pack([ev_io.getvalue(), data[1]])
    # st = read(data[0])

    kafka_handler.send_to_kafka(kafka_producer_topic, message=data_out,
                                key=key, timestamp=int(timestamp))

    # Tracer()()
    # cat = read_events(data[0])
    # cat_out = nll.run_event(cat[0].copy())
    # stream_out = BytesIO()
    # cat_out.write(stream_out, format='QUAKEML')
    # packed = msg.pack()



    # st = read(BytesIO(msg_in.value))