from loguru import logger
from microquake.focmec.core import calc_focal_mechanisms
from obspy.core.event.base import ResourceIdentifier


class Process():
    def __init__(self, app, module_settings):
        self.app = app
        self.module_settings = module_settings

    def process(
        self,
        cat=None,
        stream=None,
    ):
        save_figs = True

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

            if save_figs:
                for i, fig in enumerate(figs):
                    fig.savefig('foc_mech_%d.png' % i)

        return cat_out, stream
