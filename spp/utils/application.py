from abc import abstractmethod
from io import BytesIO

from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

from loguru import logger
from microquake.core import read_events
from microquake.io import msgpack

from ..core.settings import settings
from ..core.utils import timing


class Application(object):

    def __init__(self, step_number=None, module_name=None,
                 processing_flow_name='automatic', **kwargs):
        """
        :param module_name: name of the module, the name must be coherent
        with a section in the config file.
        :param processing_flow: Name of the processing flow. This must
        correspond to a section in the config file
        :param processing_flow_name: initialize the processing flow by
        setting the processing step to 0.
        :return: None
        """
        self.settings = settings

        self.__module_name__ = module_name

        self.step_number = step_number

        processing_flow = self.settings.get('processing_flow')[processing_flow_name]
        steps = []
        for idx, step in enumerate(processing_flow.steps):
            s = defaultdict(lambda: None, step)

            input = f"{processing_flow_name}.{s['module']}.{idx}"
            output = f"{processing_flow_name}.{s['module']}.{idx+1}"

            if idx == (len(processing_flow.steps)-1):
                output = f"{processing_flow_name}.{s['module']}.output"

            s['input'] = s['input'] or input
            s['output'] = s['output'] or output

            env = settings.current_env.lower()
            s['input'] = f"seismic_platform.{env}.{s['input']}"
            s['output'] = f"seismic_platform.{env}.{s['output']}"

            steps.append(s)

        self.processing_flow_steps = steps

    def get_consumer_topic(self, processing_flow, module_name, input_data_name=None):
        return self.processing_flow_steps[self.step_number]['input']

    def get_producer_topic(self, module_name):
        return self.processing_flow_steps[self.step_number]['output']

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
        msg = self.serialise_message(cat, stream)
        logger.info('done preparing data')

        return msg

    @abstractmethod
    def receive_message(self, msg_in, processor, **kwargs):
        """
        receive message
        """
        cat, stream = self.deserialise_message(msg_in)

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

    @timing
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
