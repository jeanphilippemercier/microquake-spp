from ..classifier.seismic_classifier import seismic_classifier_model
from .processing_unit import ProcessingUnit
from microquake.core.stream import Stream
from loguru import logger
import numpy as np

from ..core.settings import settings

class Processor(ProcessingUnit):
    @property
    def module_name(self):
        return "clean_data"

    def process(self, **kwargs):
        """
            Process event and returns its classification.
        """
        stream = kwargs["stream"]
        black_list = settings.get('sensors').black_list

        trs = []

        for i, tr in enumerate(stream):
            if tr.stats.station not in black_list:
                tr.data = np.nan_to_num(tr.data)
                if ((np.sum(tr.data ** 2) > 0)):
                    trs.append(tr)

        logger.info('The seismograms have  been cleaned, %d trace remaining' %
                    len(trs))

        return Stream(traces=trs)


    # def legacy_pipeline_handler(self, msg_in, res):
    #     """
    #         legacy pipeline handler
    #     """
    #     cat, stream = self.app.deserialise_message(msg_in)
    #     cat = self.output_catalog(cat)
    #     return cat, stream