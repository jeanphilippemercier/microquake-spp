from microquake.core import read_events
from microquake.core.stream import read

from .application import Application


class TestApplication(Application):
    def __init__(
        self,
        toml_file=None,
        module_name=None,
        processing_flow_name=None,
        input_data=None,
    ):
        super(TestApplication, self).__init__(
            toml_file=toml_file,
            module_name=module_name,
            processing_flow_name=processing_flow_name,
        )
        self.input_data = input_data
        self.logger.info("running tests for module %s", module_name)

    def get_message(self):
        catalog, waveform_stream = self.input_data
        return self.serialise_message(catalog, waveform_stream)


    def send_message(self, cat, stream, topic=None):
        msg_bytes = super(TestApplication, self).send_message(cat, stream)
        self.output_bytes = msg_bytes
        self.output_data = (cat, stream)


    def receive_message(self, msg_in, callback, **kwargs):
        return super(TestApplication, self).receive_message(msg_in, callback, **kwargs)
