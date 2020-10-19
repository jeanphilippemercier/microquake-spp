from datetime import datetime, timezone
from shutil import copyfile

from spp.core.helpers.logging import logger
from microquake.core import read_events
from microquake.core.stream import read

from .application import Application


class TestApplication(Application):
    def __init__(
        self,
        module_name=None,
        processing_flow_name=None,
        input_data=None,
    ):
        super(TestApplication, self).__init__(
            module_name=module_name,
            processing_flow_name=processing_flow_name,
        )
        self.input_data = input_data
        logger.info("running tests for module {}", module_name)

    def get_message(self):
        catalog, waveform_stream = self.input_data

        return self.serialise_message(catalog, waveform_stream)

    def send_message(self, cat, stream, topic=None):
        msg_bytes = super(TestApplication, self).send_message(cat, stream)
        self.output_bytes = msg_bytes
        self.output_data = self.clean_message((cat, stream))

    def receive_message(self, msg_in, callback, **kwargs):
        return super(TestApplication, self).receive_message(msg_in, callback, **kwargs)

    def clean_message(self, msg_in):
        msg_in = super(TestApplication, self).clean_message(msg_in)
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
