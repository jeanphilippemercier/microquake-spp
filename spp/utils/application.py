import os
from abc import abstractmethod
from io import BytesIO
from time import time

import matplotlib.pyplot as plt
import numpy as np

from loguru import logger
from microquake.core import AttribDict, read_events
from microquake.core.event import Origin
from microquake.io import msgpack

from ..core.settings import settings


def interloc_cat(x, y, z, vmax, normed_vmax, imax, iot, time, method, cat):
    cat[0].origins.append(
        Origin(x=x, y=y, z=z, time=time,
               method_id=method, evalution_status="preliminary",
               evaluation_mode="automatic")
    )
    cat[0].preferred_origin_id = cat[0].origins[-1].resource_id.id

    logger.info("power: %.3f, ix_grid: %d, ix_ot: %d" % (vmax, imax, iot))
    logger.info("utm_loc: %d %d %d", x, y, z)

    logger.info("=======================================\n")
    logger.info(
        "VMAX over threshold (%.3f)" % (vmax))

    cat[0].preferred_origin().extra.interloc_vmax \
        = AttribDict({'value': vmax, 'namespace': 'MICROQUAKE'})

    cat[0].preferred_origin().extra.interloc_normed_vmax \
        = AttribDict({'value': normed_vmax, 'namespace': 'MICROQUAKE'})

    return cat


class Application(object):

    def __init__(self, toml_file=None, module_name=None,
                 processing_flow_name=None):
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

        if self.__module_name__ is None:
            # print("No module name, application cannot initialise")
            pass

        if processing_flow_name:
            processing_flow = self.settings.get('processing_flow')[processing_flow_name]
            self.trigger_data_name = processing_flow.trigger_data_name
            self.dataset = processing_flow.dataset
            self.processing_flow_steps = processing_flow.steps

    def get_consumer_topic(self, processing_flow, dataset, module_name, trigger_data_name, input_data_name=None):
        if input_data_name:
            return self.get_topic(dataset, input_data_name)

        if module_name == 'chain':
            return self.get_topic(dataset, trigger_data_name)

        if len(processing_flow) == 0:
            raise ValueError("Empty processing_flow, cannot determine consumer topic")
        processing_step = -1

        for i, flow_step in enumerate(processing_flow):
            if module_name in flow_step:
                processing_step = i

                break

        if self.get_output_data_name(module_name) == trigger_data_name:
            # This module is triggering the processing flow. It is not consuming on any topics.

            return ""

        if processing_step == -1:
            raise ValueError(
                "Module {} does not exist in processing_flow, cannot determine consumer topic".format(module_name))

        if processing_step == 0:
            # The first module always consumes from the triggering

            return self.get_topic(dataset, trigger_data_name)

        if len(processing_flow) < 2:
            raise ValueError("Processing flow is malformed and only has one step {}".format(processing_flow))
        input_module_name = processing_flow[processing_step - 1][0]
        input_data_name = self.get_output_data_name(input_module_name)

        return self.get_topic(dataset, input_data_name)

    def get_producer_topic(self, dataset, module_name):
        return self.get_topic(dataset, self.get_output_data_name(module_name))

    def get_topic(self, dataset, data_name):
        return "seismic_processing.{}.{}".format(dataset, data_name)

    def get_output_data_name(self, module_name):
        if self.settings.get(module_name):
            return self.settings.get(module_name).output_data_name
        else:
            return ""

    @property
    def nll_tts_dir(self):
        """
        returns the path where the travel time grids are stored
        :return: path
        """

        return os.path.join(self.settings.nll_base, 'time')

    def get_ttable_h5(self):
        from microquake.core.data import ttable
        fname = os.path.join(settings.common_dir,
                             settings.grids.travel_time_h5.fname)

        return ttable.H5TTable(fname)

    def write_ttable_h5(self, fname=None):
        from microquake.core.data import ttable

        if fname is None:
            fname = settings.grids.travel_time_h5.fname

        ttp = ttable.array_from_nll_grids(self.nll_tts_dir, 'P', prefix='OT')
        tts = ttable.array_from_nll_grids(self.nll_tts_dir, 'S', prefix='OT')
        fpath = os.path.join(settings.common_dir, fname)
        ttable.write_h5(fpath, ttp, tdict2=tts)

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
        logger.info("processing event %s", str(cat[0].resource_id))

        if processor.module_name == 'interloc':
            x, y, z, vmax, normed_vmax, imax, iot, utctime, method = processor.process(stream=stream)
            # TODO: check if pass by reference
            interloc_cat(x, y, z, vmax, normed_vmax, imax, iot, utctime, method, cat)
            _, st_out = self.deserialise_message(msg_in)
            cat_out = cat

        elif not kwargs:
            cat_out, st_out = processor.process(cat=cat, stream=stream)
        else:
            cat_out, st_out = processor.process(cat=cat, stream=stream,
                                                **kwargs)

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
    #ax.plot(x, y, 'b')
    ax.plot(h, z, 'b')
    plt.show()
    print("sta:%s phase:%s node.x[-2]=%f node.x[-1]=%f" % (sta_code, phase, nodes[-2][0], nodes[-1][0]))
    print("sta:%s phase:%s node.y[-2]=%f node.y[-1]=%f" % (sta_code, phase, nodes[-2][1], nodes[-1][1]))

    print("N nodes:%d" % len(nodes))
