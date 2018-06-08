from spp.ims_connector import core
from importlib import reload
from scripts.kafka_utils import KafkaHandler
from io import BytesIO
import sys
import time
import threading

reload(core)


def write_to_kafka(kafka_handler_obj, kafka_topic, stream_object):
    s_time = time.time()
    buf = BytesIO()
    stream_object.write(buf, format='MSEED')
    encoded_obj = serializer.encode_base64(buf)
    msg_key = str(stream_object[0].stats.starttime)
    e_time = time.time() - s_time
    print("object preparation took:", e_time)
    print("sending to kafka...", "key:", msg_key, "msg size:", sys.getsizeof(encoded_obj) / 1024 / 1024, "MB")
    s_time = time.time()
    kafka_handler_obj.send_to_kafka(kafka_topic, serializer.encode_base64(buf), msg_key.encode('utf-8'))
    e_time = time.time() - s_time
    print("object submission took:", e_time)


def send_list_to_kafka(kafka_handler_obj, kafka_topic, st_list):
    for stream_object in st_list:
        write_to_kafka(kafka_handler_obj, kafka_topic, stream_object)

    print("Flushing and Closing Kafka....")
    kafka_handler_obj.producer.flush()


def run_kafka_threads(streams_list, kafka_topic, kafka_brokers):
    # run a kafka producer thread for each array of streams
    threads_list = []
    for i in range(len(streams_list)):
        print("Starting Thread #", i)
        threads_list.append(threading.Thread(target=send_list_to_kafka, args=(KafkaHandler(kafka_brokers),
                                                                              kafka_topic, streams_list[i])))
        threads_list[i].start()

    # wait for all threads to finish
    for th in threads_list:
        th.join()


def initialize_streams_list(threads_cnt):
    lst = []
    for i in range(threads_cnt):
        lst.append([])
    return lst


if __name__ == "__main__":

    # read yaml file

    import os
    import yaml
    from microquake.core.event import Catalog
    from microquake.core.util import serializer
    import numpy as np

    config_dir = os.environ['SPP_CONFIG']
    #common_dir = os.environ['SPP_COMMON']

    fname = os.path.join(config_dir, 'ims_connector_config.yaml')

    with open(fname, 'r') as cfg_file:
        params = yaml.load(cfg_file)
        params = params['ims_connector']


    # Kafka configs
    kafka_brokers = params['kafka']['brokers']
    kafka_topic = params['kafka']['topic']

    threads_count = params['kafka']['threads']

    stime = time.time()

    # creating different lists for threads
    streams_list = initialize_streams_list(threads_count)

    if params['data_source']['type'] == 'remote':
        j = 0
        for st in core.request_handler():
            print(st)
            # write to Kafka
            streams_list[j % threads_count].append(st)
            j += 1

    elif params['data_source']['type'] == 'local':
        location = params['data_source']['location']
        period = params['period']
        window_length = params['window_length']

        # distribute the streams on the lists
        for i in np.arange(0, period, window_length):
            st = core.request_handler_local(location)
            print("(", i, " from", period, ")")
            print("---------Putting in List #------------------> ", i % threads_count)
            streams_list[i % threads_count].append(st)

    run_kafka_threads(streams_list, kafka_topic, kafka_brokers)

    etime = time.time()
    print("==> Total Time Taken: ", etime - stime)

    print("Program Exit")


