from loguru import logger
from microquake.waveform.amp_measures import calc_velocity_flux
from microquake.waveform.mag import calculate_energy_from_flux

from ..core.settings import settings


class Processor():
    def __init__(self, module_name, app=None, module_type=None):
        self.__module_name = module_name
        self.params = settings.get(self.module_name)

    @property
    def module_name(self):
        return self.__module_name

    def process(
        self,
        **kwargs
    ):
        cat = kwargs["cat"]
        stream = kwargs["stream"]

        correct_attenuation = self.params.correct_attenuation
        Q = self.params.attenuation_Q
        use_sdr_rad = self.params.use_sdr_rad

        if use_sdr_rad and cat.preferred_focal_mechanism() is None:
            logger.warning("use_sdr_rad=True but preferred focal mech = None --> Setting use_sdr_rad=False")
            use_sdr_rad = False

        phase_list = self.params.phase_list

        if not isinstance(phase_list, list):
            phase_list = [phase_list]

        cat_out = cat.copy()
        st = stream.copy()

        missing_responses = st.attach_response(settings.inventory)

        for sta in missing_responses:
            logger.warning("Inventory: Missing response for sta:%s" % sta)

        calc_velocity_flux(st,
                           cat_out,
                           phase_list=phase_list,
                           correct_attenuation=correct_attenuation,
                           Q=Q,
                           debug=False,
                           logger_in=logger)

        calculate_energy_from_flux(cat_out,
                                   use_sdr_rad=use_sdr_rad,
                                   logger_in=logger)

        return cat_out, st
