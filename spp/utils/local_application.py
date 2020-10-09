from datetime import datetime, timezone
from shutil import copyfile

from microquake.core import read_events
from microquake.core.stream import read

from .application import Application

from spp.core.helpers.logging import logger

class LocalApplication(Application):
    def __init__(
        self,
        module_name=None,
        processing_flow_name="automatic",
        input_bytes=None,
        input_mseed=None,
        input_quakeml=None,
        output_bytes=None,
        output_mseed=None,
        output_quakeml=None,
    ):
        super(LocalApplication, self).__init__(
            module_name=module_name,
            processing_flow_name=processing_flow_name,
        )
        self.input_bytes = input_bytes
        self.input_mseed = input_mseed
        self.input_quakeml = input_quakeml
        self.output_bytes = output_bytes
        self.output_mseed = output_mseed
        self.output_quakeml = output_quakeml
        logger.info("running module locally")

    def read_local_data(
        self,
        input_bytes=None,
        input_mseed=None,
        input_quakeml=None,
    ):
        if input_bytes is not None:
            with open(input_bytes, "rb") as data:
                catalog, waveform_stream = self.deserialise_message(data.read())
        elif input_mseed is not None and input_quakeml is not None:
            with open(input_quakeml, "rb") as event_file:
                catalog = read_events(event_file, format="QUAKEML")

            with open(input_mseed, "rb") as event_file:
                waveform_stream = read(event_file, format="MSEED")
        else:
            raise ValueError("Cannot load local data, no input files specified")

        return catalog, waveform_stream

    def save_local_data(
        self,
        catalog,
        waveform_stream,
        msg_bytes,
        output_bytes=None,
        output_mseed=None,
        output_quakeml=None,
    ):
        if output_bytes is not None:
            with open(output_bytes, "wb") as output_file:
                output_file.write(msg_bytes)

        if output_mseed is not None:
            waveform_stream.write(output_mseed)

        if output_quakeml is not None:
            catalog.write(output_quakeml, format="QUAKEML")

    def get_message(self):
        catalog, waveform_stream = self.read_local_data(
            self.input_bytes, self.input_mseed, self.input_quakeml
        )
        return self.serialise_message(catalog, waveform_stream)

    def send_message(self, cat, stream, topic=None):
        msg_bytes = super(LocalApplication, self).send_message(cat, stream)
        self.save_local_data(
            cat,
            stream,
            msg_bytes,
            self.output_bytes,
            self.output_mseed,
            self.output_quakeml,
        )

    def receive_message(self, msg_in, processor, **kwargs):
        return super(LocalApplication, self).receive_message(msg_in, processor, **kwargs)

    def clean_message(self, msg_in):
        msg_in = super(LocalApplication, self).clean_message(msg_in)
        (catalog, waveform_stream) = msg_in


        # If the catalog is copied too many times in a row, it will cause the
        # arrivals to lose their reference to pick_id and obspy creates new ones
        # (that don't point to any picks).
        # Write and then read the catalog here locally to avoid this.

        logger.info("Testing, writing to local file to reset category")
        tmp_location = './tmp_test_cat.xml'
        tmp_copy_location = './tmp_test_cat_%s.xml'.format(int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        catalog.write(tmp_location, format="QUAKEML")
        copyfile(tmp_location, tmp_copy_location)
        with open(tmp_copy_location, "rb") as event_file:
            new_catalog = read_events(event_file, format="QUAKEML")
            logger.info("Testing, reading from local file to reset category")

        return (new_catalog, waveform_stream)
