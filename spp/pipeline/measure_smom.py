from loguru import logger
from microquake.waveform.smom_measure import measure_pick_smom
from obspy.core.event.base import Comment


class Processor():
    def __init__(self, app, module_settings):
        self.app = app
        self.module_settings = module_settings
        self.inventory = app.get_inventory()

    def process(
        self,
        cat=None,
        stream=None,
    ):

        params = self.module_settings
        use_fixed_fmin_fmax = params.use_fixed_fmin_fmax
        fmin = params.fmin
        fmax = params.fmax
        phase_list = params.phase_list

        if not isinstance(phase_list, list):
            phase_list = [phase_list]

        plot_fit = False

        st = stream.copy()
        cat_out = cat.copy()

        missing_responses = st.attach_response(self.inventory)

        for sta in missing_responses:
            logger.warning("Inventory: Missing response for sta:%s" % sta)

        for event in cat_out:
            origin = event.preferred_origin()
            synthetic_picks = self.app.synthetic_arrival_times(origin.loc, origin.time)

            for phase in phase_list:

                logger.info("Call measure_pick_smom for phase=[%s]" % phase)

                try:
                    smom_dict, fc = measure_pick_smom(st, self.inventory, event,
                                                      synthetic_picks,
                                                      P_or_S=phase,
                                                      fmin=fmin, fmax=fmax,
                                                      use_fixed_fmin_fmax=use_fixed_fmin_fmax,
                                                      plot_fit=plot_fit,
                                                      debug_level=1,
                                                      logger_in=logger)
                except Exception as e:
                    logger.warn("Error in measure_pick_smom. Continuing to next phase in phase_list: \n %s", e)

                    continue

                comment = Comment(text="corner_frequency_%s=%.2f measured for %s arrivals" %
                                  (phase, fc, phase))
                origin.comments.append(comment)

        return cat_out, stream
