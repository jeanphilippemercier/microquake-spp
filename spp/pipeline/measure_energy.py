from microquake.waveform.amp_measures import calc_velocity_flux
from microquake.waveform.mag import calculate_energy_from_flux

def process(cat=None,
            stream=None,
            logger=None,
            app=None,
            module_settings=None,
            prepared_objects=None,
           ):

    # reading application data
    params = app.settings.measure_energy

    if logger is None:
        logger = app.logger

    correct_attenuation = params.correct_attenuation
    Q = params.attenuation_Q
    use_sdr_rad = params.use_sdr_rad

    if use_sdr_rad and cat.preferred_focal_mechanism() is None:
        logger.warn("use_sdr_rad=True but preferred focal mech = None --> Setting use_sdr_rad=False")
        use_sdr_rad = False

    phase_list = params.phase_list
    if not isinstance(phase_list, list):
        phase_list = [phase_list]

    cat_out = cat.copy()
    st = stream.copy()

    inventory = app.get_inventory()
    missing_responses = st.attach_response(inventory)
    for sta in missing_responses:
        logger.warn("Inventory: Missing response for sta:%s" % sta)

    calc_velocity_flux(st,
                       cat_out,
                       phase_list=phase_list,
                       correct_attenuation=correct_attenuation,
                       Q=Q,
                       debug=False,
                       logger_in=logger)

    calculate_energy_from_flux(cat_out, use_sdr_rad=use_sdr_rad)

    return cat_out, st


