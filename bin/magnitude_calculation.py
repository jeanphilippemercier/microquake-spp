from spp.utils.application import Application
from spp.utils.kafka import KafkaHandler
from microquake.waveform.mag import moment_magnitude

conf = Application()
settings = conf.settings

kafka_brokers = settings.kafka.brokers
kafka_topic = settings.magnitude.kafka_consumer_topic

kafka_handle = KafkaHandler(kafka_brokers)
consumer = KafkaHandler.consume_from_topic(kafka_topic, kafka_brokers)



moment_magnitude(stream, evt, site, vp, vs, ttpath=None,
                 only_triaxial=True, density=2700, min_dist=20,
                 win_length=0.02, len_spectrum=2 ** 14, freq=100)

