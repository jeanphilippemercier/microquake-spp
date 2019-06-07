from loguru import logger
from microquake.waveform.smom_measure import measure_pick_smom
from obspy.core.event.base import Comment

from ..core.grid import synthetic_arrival_times
from ..core.settings import settings


class Processor():
    def __init__(self, module_name, app=None, module_type=None):
        self.__module_name = module_name
        self.params = settings.get(self.module_name)

        self.use_fixed_fmin_fmax = self.params.use_fixed_fmin_fmax
        self.fmin = self.params.fmin
        self.fmax = self.params.fmax
        self.phase_list = self.params.phase_list

    @property
    def module_name(self):
        return self.__module_name

    def process(
        self,
        **kwargs
    ):
        """
        input: catalog, stream

        - origin and picks


        list of corner frequencies for the arrivals
        returns catalog
        """

        cat = kwargs["cat"]
        stream = kwargs["stream"]

        if not isinstance(self.phase_list, list):
            phase_list = [self.phase_list]

        plot_fit = False

        st = stream.copy()
        cat_out = cat.copy()

        missing_responses = st.attach_response(settings.inventory)

        for sta in missing_responses:
            logger.warning("Inventory: Missing response for sta:%s" % sta)

        for event in cat_out:
            origin = event.preferred_origin()
            synthetic_picks = synthetic_arrival_times(origin.loc, origin.time)

            for phase in phase_list:

                logger.info("Call measure_pick_smom for phase=[%s]" % phase)

                try:
                    smom_dict, fc = measure_pick_smom(st, settings.inventory, event,
                                                      synthetic_picks,
                                                      P_or_S=phase,
                                                      fmin=self.fmin, fmax=self.fmax,
                                                      use_fixed_fmin_fmax=self.use_fixed_fmin_fmax,
                                                      plot_fit=plot_fit,
                                                      debug_level=1,
                                                      logger_in=logger)
                except Exception as e:
                    logger.warning("Error in measure_pick_smom. Continuing to next phase in phase_list: \n %s", e)

                    continue

                comment = Comment(text="corner_frequency_%s=%.2f measured for %s arrivals" %
                                  (phase, fc, phase))
                origin.comments.append(comment)

        return cat_out, stream
