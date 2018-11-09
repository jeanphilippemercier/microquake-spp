from microquake.io import msgpack
from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from io import BytesIO

app = Application()
settings = app.settings
logger = app.get_logger()

project_code = settings.project_code
base_folder = settings.nlloc.nll_base

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.nlloc.kafka_consumer_topic

kafka_handler = KafkaHandler(kafka_brokers)

pfle = 'pack.dat'
f = open(pfle, 'rb')
buf = f.read()
data = msgpack.unpack(buf)

logger.info("Here")
kafka_handler.send_to_kafka(kafka_topic, message=fout, key="test", timestamp=1)
kafka_handler.producer.flush()


data = msgpack.unpack(buf)



