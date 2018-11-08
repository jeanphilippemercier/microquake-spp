from spp.utils.config import Configuration
from spp.utils.kafka import KafkaHandler
from microquake.nlloc import NLL

conf = Configuration()
settings = conf.settings

# initialize NLL object

# check if velocity is current
# if not download from server
# and run nll_prepare

project_code = settings.project_code
base_folder = settings.nlloc.nll_base
gridpar = conf.nll_velgrids()
sensors = conf.nll_sensors()
params = conf.settings.nlloc

nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
          sensors=sensors, params=params)

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.nlloc.kafka_consumer_topic

kafka_handle = KafkaHandler(kafka_brokers)
consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
    print("Received Key:", msg_in.key)
    st = read(BytesIO(msg_in.value))





