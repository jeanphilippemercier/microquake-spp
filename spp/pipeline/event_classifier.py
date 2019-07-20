from ..classifier.seismic_classifier import SeismicClassifierModel
from .processing_unit import ProcessingUnit

class Processor(ProcessingUnit):
    @property
    def module_name(self):
        return "event_classifier"

    def initializer(self):
        self.seismic_model = SeismicClassifierModel()

    def process(self, **kwargs):
        """
            Process event and returns its classification.
        """
        stream = kwargs["stream"]
        height = kwargs["height"]
        self.seismic_model.create_model()
        self.response = self.seismic_model.predict(stream, hour)
        return self.response

    def legacy_pipeline_handler(self, msg_in, res):
        """
            legacy pipeline handler
        """
        cat, stream = self.app.deserialise_message(msg_in)
        cat = self.output_catalog(cat)
        return cat, stream
