from spp.ims_connector import core
from importlib import reload
from scripts.kafka_utils import KafkaHandler
from io import BytesIO
import logging

reload(core)


def write_to_kafka(kafka_handler_obj, kafka_topic, stream_object):
    s_time = time.time()
    buf = BytesIO()
    stream_object.write(buf, format='MSEED')
    print("==> Normal:", sys.getsizeof(buf) / 1024 / 1024)
    kafka_msg = buf.getvalue() #serializer.encode_base64(buf)
    msg_key = str(stream_object[0].stats.starttime)
    e_time = time.time() - s_time
    print("==> object preparation took:", e_time)
    print("==> sending to kafka...", "key:", msg_key, "msg size:", sys.getsizeof(kafka_msg) / 1024 / 1024, "MB")
    s_time = time.time()
    kafka_handler_obj.send_to_kafka(kafka_topic, kafka_msg, msg_key.encode('utf-8'))
    e_time = time.time() - s_time
    print("==> object submission took:", e_time)

if __name__ == "__main__":

    # read yaml file
    logging.basicConfig(level=logging.ERROR)
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
            print("==> (", i, " from", period, ")")
            s_time = time.time()
            st = core.request_handler_local(location)
            e_time = time.time() - s_time
            print("==> Fetching File took: ", e_time)
            #print("---------------------------> ", type(st))
            # print(st)
            #  write to Kafka
            write_to_kafka(kafka,kafka_topic, st)


        print("==> Flushing and Closing Kafka....")
        s_time = time.time()
        kafka.producer.flush()
        e_time = time.time() - s_time
        print("==> Flushing Kafka took: ", e_time)

        etime = time.time()
        print("==> Total Time Taken: ", etime - stime)

        print("==> Program Exit")


