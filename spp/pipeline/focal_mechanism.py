from loguru import logger
from microquake.focmec.core import calc_focal_mechanisms
from obspy.core.event.base import ResourceIdentifier

from .processing_unit import ProcessingUnit


class Processor(ProcessingUnit):
    def initializer(self):
        self.save_figs = True

    def process(
        self,
        **kwargs
    ):
        """
        Needs the catalog
        - Origin
        - Picks and Arrivals associated to the Origin

        Returns the focal mechanism
        - list of angles
        - two focal planes
        """

        cat = kwargs["cat"]
        stream = kwargs["stream"]

        cat_out = cat.copy()

        focal_mechanisms, figs = calc_focal_mechanisms(cat, self.params,
                                                       logger_in=logger)

        if len(focal_mechanisms) > 0:
            for i, event in enumerate(cat_out):
                focal_mechanism = focal_mechanisms[i]
                event.focal_mechanisms = [focal_mechanism]
                event.preferred_focal_mechanism_id = ResourceIdentifier(id=focal_mechanism.resource_id.id,
                                                                        referred_object=focal_mechanism)
                logger.info(event.preferred_focal_mechanism())

            if self.save_figs:
                for i, fig in enumerate(figs):
                    fig.savefig('foc_mech_%d.png' % i)

        return {'cat': cat_out, 'stream': stream}

    def legacy_pipeline_handler(
        self,
        msg_in,
        res
    ):
        return res['cat'], res['stream']
