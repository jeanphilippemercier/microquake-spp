# This script test the recomposition of mseed from the messages consumed
from spp.utils.kafka import KafkaHandler
from microquake.core import read
from io import BytesIO

kafka_brokers = "kafka-node-001:9092,kafka-node-002:9092,kafka-node-003:9092"#['localhost:9092']
kafka_topic = "mseed_samples_6" #'data_ingestion'

kafka_handle = KafkaHandler(kafka_brokers)

consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)

print("Awaiting Kafka mseed messsages")
for msg_in in consumer:
    print("Received Key:", msg_in.key)
    st = read(BytesIO(msg_in.value))
    print(st)

