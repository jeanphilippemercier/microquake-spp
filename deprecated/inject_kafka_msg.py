from spp.utils.kafka import KafkaHandler
import numpy as np
import os
import struct
import yaml
from glob import glob
from microquake.core import read_events
from spp.utils.application import Application

def main():

    DATA_DIR = '/Users/mth/mth/Data/OT_data/'    # Move to env ?

    intensity = -2.0
    run_from_xml = True
    run_from_xml = False
    if run_from_xml:
        event_files = glob(DATA_DIR + "20180706112101.xml")

        for xmlfile in event_files:
            event = read_events(xmlfile, format='QUAKEML')[0]
            origin = event.origins[0]
            inputs = np.array([origin.time.timestamp, *origin.loc.tolist(), intensity])
    else:
        inputs = np.array( [ 1.53087606e+09, 6.51185000e+05, 4.76742700e+06, -1.48000000e+02, -2.00000000e+00])

    config = Application()

    # Create Kafka Object
    kafka_brokers = config.DATA_CONNECTOR['kafka']['brokers']
    kafka_topic = 'interloc' # PAF - needs to be made configurable

    #consumer = KafkaHandler.consume_from_topic(kafka_topic,kafka_brokers)

    #msg = np.array([ot_epoch, loc[0], loc[1], loc[2], power], dtype=np.float64)
    msg = inputs
    kaf_msg = struct.pack('%sd' % len(msg), *msg)
    kaf_key = ("iloc_%d" % (inputs[0])).encode('utf-8')
    #kaf_key = ("iloc_%d" % (origin.time.timestamp)).encode('utf-8')
    #kaf_key = ("iloc_%d" % (ot_epoch)).encode('utf-8')

    print("Sending Kafka interloc messsage. kaf_msg:%s key=[%s]" % (inputs, kaf_key))
    #msg_out, key_out = xflow.encode_for_kafka(ot_epoch, lmax, vmax)
    kafka_handler_obj = KafkaHandler(kafka_brokers)
    kafka_handler_obj.send_to_kafka(kafka_topic, kaf_msg, kaf_key)
    kafka_handler_obj.producer.flush()
    print("==================================================================")

if __name__ == "__main__":
    main()
