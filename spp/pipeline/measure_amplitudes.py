from loguru import logger
from microquake.core.stream import Stream
from microquake.waveform.amp_measures import measure_pick_amps
from microquake.waveform.transforms import rotate_to_ENZ, rotate_to_P_SV_SH


class Processor():
    def __init__(self, app, module_settings):
        self.app = app
        self.module_settings = module_settings

    def process(
        self,
        cat=None,
        stream=None,
    ):

        pulse_min_width = self.module_settings.pulse_min_width
        pulse_min_snr_P = self.module_settings.pulse_min_snr_P
        pulse_min_snr_S = self.module_settings.pulse_min_snr_S
        phase_list = self.module_settings.phase_list

        if not isinstance(phase_list, list):
            phase_list = [phase_list]

        st = stream.copy()
        cat_out = cat.copy()

        inventory = self.app.get_inventory()
        missing_responses = st.attach_response(inventory)

        for sta in missing_responses:
            logger.warning("Inventory: Missing response for sta:%s" % sta)

        # 1. Rotate traces to ENZ
        st_rot = rotate_to_ENZ(st, inventory)
        st = st_rot

        # 2. Rotate traces to P,SV,SH wrt event location
        st_new = rotate_to_P_SV_SH(st, cat_out)
        st = st_new

        # 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
        trP = [tr for tr in st if tr.stats.channel == 'P' or tr.stats.channel.upper() == 'Z']

        measure_pick_amps(Stream(traces=trP),
                          # measure_pick_amps(st_rot,
                          cat_out,
                          phase_list=phase_list,
                          pulse_min_width=pulse_min_width,
                          pulse_min_snr_P=pulse_min_snr_P,
                          pulse_min_snr_S=pulse_min_snr_S,
                          debug=False,
                          logger_in=logger)

        return cat_out, stream
