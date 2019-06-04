from loguru import logger
from microquake.waveform.amp_measures import calc_velocity_flux
from microquake.waveform.mag import calculate_energy_from_flux

from ..core.settings import settings


class Processor():
    def __init__(self, app, module_settings):
        self.module_settings = module_settings

    def process(
        self,
        cat=None,
        stream=None,
    ):
        # reading application data
        params = settings.get('measure_energy')

        correct_attenuation = params.correct_attenuation
        Q = params.attenuation_Q
        use_sdr_rad = params.use_sdr_rad

        if use_sdr_rad and cat.preferred_focal_mechanism() is None:
            logger.warning("use_sdr_rad=True but preferred focal mech = None --> Setting use_sdr_rad=False")
            use_sdr_rad = False

        phase_list = params.phase_list

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
