from obspy.core.event.base import ResourceIdentifier

from loguru import logger
from microquake.focmec.core import calc_focal_mechanisms


class Processor():
    def __init__(self, module_name, app=None, module_type=None):
        self.__module_name = module_name
        self.save_figs = True

    @property
    def module_name(self):
        return self.__module_name

    def process(
        self,
        cat=None,
        stream=None,
    ):
        """
        Needs the catalog
        - Origin
        - Picks and Arrivals associated to the Origin

        Returns the focal mechanism
        - list of angles
        - two focal planes
        """

        cat_out = cat.copy()

        logger.info("Calculate focal mechanisms")

        focal_mechanisms, figs = calc_focal_mechanisms(cat_out, self.module_settings, logger_in=logger)

        logger.info("Calculate focal mechanisms [DONE]")

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

        return cat_out, stream
