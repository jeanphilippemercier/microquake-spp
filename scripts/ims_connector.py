from spp.ims_connector import core
from importlib import reload
from scripts.kafka_utils import KafkaHandler
from io import BytesIO

reload(core)


def write_to_kafka(kafka_handler_obj, kafka_topic, stream_object):
    buf = BytesIO()
    stream_object.write(buf, format='MSEED')
    encoded_obj = serializer.encode_base64(buf)
    msg_key = str(stream_object[0].stats.starttime)
    print("sending to kafka...", "key:", msg_key, "msg size:", sys.getsizeof(encoded_obj) / 1024 / 1024, "MB")
    kafka_handler_obj.send_to_kafka(kafka_topic, serializer.encode_base64(buf), msg_key.encode('utf-8'))


if __name__ == "__main__":

    # read yaml file

    import os
    import yaml
    from microquake.core.event import Catalog
    from microquake.core.util import serializer
    import numpy as np

    config_dir = os.environ['SPP_CONFIG']
    common_dir = os.environ['SPP_COMMON']

    fname = os.path.join(config_dir, 'ims_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['ims_connector']


    # Create Kafka Object
    kafka = KafkaHandler(params['kafka']['brokers'])
    kafka_topic = params['kafka']['topic']

    if params['data_source']['type'] == 'remote':
        for st in core.request_handler():
            print(st)
            # write to Kafka
            write_to_kafka(kafka, kafka_topic, st)

    elif params['data_source']['type'] == 'local':
        location = params['data_source']['location']
        period = params['period']
        window_length = params['window_length']

        import sys
        import time

        stime = time.time()

        for i in np.arange(0, period, window_length):
            st = core.request_handler_local(location)
            print("---------------------------> ", type(st))
            # print(st)
            #  write to Kafka
            print("(", i, " from", period, ")")
            write_to_kafka(kafka,kafka_topic, st)


        print("Flushing and Closing Kafka....")
        kafka.producer.flush()

        etime = time.time()
        print("==> Total Time Taken: ", etime - stime)

        print("Program Exit")


