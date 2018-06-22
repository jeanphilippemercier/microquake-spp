import logging
from importlib import reload
from io import BytesIO

from spp.ims_connector import core
from spp.utils.kafka import KafkaHandler

reload(core)


def write_to_kafka(kafka_handler_obj, kafka_topic, stream_object):
    s_time = time.time()
    buf = BytesIO()
    stream_object.write(buf, format='MSEED')
    kafka_msg = buf.getvalue() #serializer.encode_base64(buf)
    msg_key = str(stream_object[0].stats.starttime)
    end_time_preparation = time.time() - s_time

    msg_size = (sys.getsizeof(kafka_msg) / 1024 / 1024)

    s_time = time.time()
    kafka_handler_obj.send_to_kafka(kafka_topic, kafka_msg, msg_key.encode('utf-8'))
    end_time_submission = time.time() - s_time

    print("==> Object Size:", "%.2f" % msg_size, "MB",
          "Key:", msg_key,
          ", Preparation took:", "%.2f" % end_time_preparation,
          ", Submission took:", "%.2f" % end_time_submission)

if __name__ == "__main__":

    # read yaml file
    import os
    import yaml
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
            print("==> Processing (", i, " from", period, ")")
            s_time = time.time()
            st = core.request_handler_local(location)
            e_time = time.time() - s_time
            print("==> Fetching File took: ", "%.2f" % e_time)
            #  write to Kafka
            write_to_kafka(kafka,kafka_topic, st)

        print("==> Flushing and Closing Kafka....")
        s_time = time.time()
        kafka.producer.flush()
        e_time = time.time() - s_time
        print("==> Flushing Kafka took: ", "%.2f" % e_time)

        etime = time.time() - stime()
        print("==> Total Time Taken: ", "%.2f" % etime)

        print("==> Program Exit")


