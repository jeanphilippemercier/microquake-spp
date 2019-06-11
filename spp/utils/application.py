import os
from abc import abstractmethod
from io import BytesIO
from time import time

from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

from loguru import logger
from microquake.core import read_events
from microquake.io import msgpack

from ..core.settings import settings


class Application(object):

    def __init__(self, toml_file=None, step_number=None, module_name=None,
                 processing_flow_name='automatic', **kwargs):
        """
        :param toml_file: path to the TOML file containing the project
        parameter. If not set, the function will look for a file named
        settings.toml in the $SPP_CONFIG directory
        :param module_name: name of the module, the name must be coherent
        with a section in the config file.
        :param processing_flow: Name of the processing flow. This must
        correspond to a section in the config file
        :param processing_flow_name: initialize the processing flow by
        setting the processing step to 0.
        :return: None
        """
        settings.load(toml_file)

        self.settings = settings

        self.__module_name__ = module_name

        self.step_number = step_number

        processing_flow = self.settings.get('processing_flow')[processing_flow_name]
        self.trigger_data_name = processing_flow.trigger_data_name
        self.dataset = processing_flow.dataset
        steps = []
        logger.info(len(processing_flow.steps))
        for idx, step in enumerate(processing_flow.steps):
            s = defaultdict(lambda: None, step)

            input = f"{processing_flow_name}.{s['module']}.{idx}"
            output = f"{processing_flow_name}.{s['module']}.{idx+1}"

            if idx == (len(processing_flow.steps)-1):
                output = f"{processing_flow_name}.{s['module']}.output"

            s['input'] = s['input'] or input
            s['output'] = s['output'] or output

            steps.append(s)

        self.processing_flow_steps = steps

    def get_consumer_topic(self, processing_flow, dataset, module_name, trigger_data_name, input_data_name=None):
        return self.processing_flow_steps[self.step_number]['input']

    def get_producer_topic(self, dataset, module_name):
        return self.processing_flow_steps[self.step_number]['output']

    def get_topic(self, dataset, data_name):
        return "seismic_processing.{}.{}".format(dataset, data_name)

    def get_output_data_name(self, module_name):
        if self.settings.get(module_name):
            return self.settings.get(module_name).output_data_name
        else:
            return ""

    def clean_waveform_stream(self, waveform_stream, stations_black_list):
        for trace in waveform_stream:
            if trace.stats.station in stations_black_list:
                waveform_stream.remove(trace)

        return waveform_stream

    def close(self):
        logger.info('closing application...')

    @abstractmethod
    def send_message(self, cat, stream, topic=None):
        """
        send message
        """
        logger.info('preparing data')
        msg = self.serialise_message(cat, stream)
        logger.info('done preparing data')

        return msg

    @abstractmethod
    def receive_message(self, msg_in, processor, **kwargs):
        """
        receive message
        """
        logger.info('unpacking data')
        t2 = time()
        cat, stream = self.deserialise_message(msg_in)
        t3 = time()
        logger.info('done unpacking data in %0.3f seconds' % (t3 - t2))

        (cat, stream) = self.clean_message((cat, stream))
        logger.info("processing event {}", str(cat[0].resource_id))

        res = processor.process(cat=cat, stream=stream)

        cat_out, st_out = processor.legacy_pipeline_handler(msg_in, res)

        return cat_out, st_out

    def clean_message(self, msg_in):
        (catalog, waveform_stream) = msg_in

        if self.settings.get('sensors').black_list is not None:
            self.clean_waveform_stream(
                waveform_stream, self.settings.get('sensors').black_list
            )

        return (catalog, waveform_stream)

    def serialise_message(self, cat, stream):
        ev_io = BytesIO()

        if cat is not None:
            cat[0].write(ev_io, format='QUAKEML')

        return msgpack.pack([stream, ev_io.getvalue()])

    def deserialise_message(self, data):
        stream, quake_ml_bytes = msgpack.unpack(data)
        cat = read_events(BytesIO(quake_ml_bytes), format='QUAKEML')

        return cat, stream


def plot_nodes(sta_code, phase, nodes, event_location):
    x = []
    y = []
    z = []
    h = []
    print(event_location[0], event_location[1])

    for node in nodes:
        x.append(node[0] - event_location[0])
        y.append(node[1] - event_location[1])
        z.append(node[2] - event_location[2])
        h.append(np.sqrt(x[-1]*x[-1] + y[-1]*y[-1]))
        print(x[-1], y[-1])

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.set_title("Sta:%s [%s] ray nodes - ev_loc at 0,0" % (sta_code, phase))
    ax.set_xlabel('x offset = Easting')
    ax.set_xlabel('horiz offset')
    ax.set_ylabel('y offset = Northing')
    ax.set_ylabel('z offset wrt ev dep')
    # ax.plot(x, y, 'b')
    ax.plot(h, z, 'b')
    plt.show()
    print("sta:%s phase:%s node.x[-2]=%f node.x[-1]=%f" % (sta_code, phase, nodes[-2][0], nodes[-1][0]))
    print("sta:%s phase:%s node.y[-2]=%f node.y[-1]=%f" % (sta_code, phase, nodes[-2][1], nodes[-1][1]))

    print("N nodes:%d" % len(nodes))
