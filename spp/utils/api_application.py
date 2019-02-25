from spp.utils import seismic_client

from .application import Application


class APIApplication(Application):
    def __init__(
        self,
        toml_file=None,
        module_name=None,
        processing_flow_name="automatic",
        event_id=None,
        send_to_api=False,
    ):
        super(APIApplication, self).__init__(
            toml_file=toml_file,
            module_name=module_name,
            processing_flow_name=processing_flow_name,
        )
        self.logger.info("running module with the API")
        self.event_id = event_id
        self.send_to_api = send_to_api
        self.api_base_url = self.settings.seismic_api.base_url

    def retrieve_api_data(self, event_id):
        self.logger.info("Retrieving data from web_api")
        request = seismic_client.get_event_by_id(self.api_base_url, event_id)
        if request is None:
            return None
        cat = request.get_event()
        st = request.get_waveforms()
        self.logger.info("Retrieved data from web_api")
        return cat, st

    def send_api_data(self, catalog, waveform_stream):
        self.logger.info("Sending data from web_api")
        event_id = catalog.resource_id.id
        seismic_client.post_data_from_objects(
            self.api_base_url, event_id=event_id, event=catalog, stream=waveform_stream
        )
        self.logger.info("Sent data from web_api")

    def get_message(self):
        catalog, waveform_stream = self.retrieve_api_data(self.event_id)
        return self.serialise_message(catalog, waveform_stream)

    def send_message(self, cat, stream, topic=None):
        msg_bytes = super(APIApplication, self).send_message(cat, stream)
        cat, stream = self.deserialise_message(msg_bytes)
        if self.send_to_api:
            self.send_api_data(cat, stream)

    def receive_message(self, msg_in, callback, **kwargs):
        return super(APIApplication, self).receive_message(msg_in, callback, **kwargs)
