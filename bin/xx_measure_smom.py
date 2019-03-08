
from obspy.core.event.base import Comment

from microquake.core import read
from microquake.core.data.inventory import inv_station_list_to_dict

from microquake.core.event import read_events
from microquake.core.stream import Stream
from microquake.waveform.amp_measures import measure_pick_amps
from microquake.waveform.smom_mag import measure_pick_smom
from microquake.waveform.transforms import rotate_to_ENZ, rotate_to_P_SV_SH
from spp.utils.application import Application
from spp.utils.seismic_client import get_event_by_id

from lib_process import processCmdLine

def main():

    fname = 'xx_measure_smom'
    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname)

    # reading application data
    app = Application()
    settings = app.settings

    logger = app.get_logger(fname,'zlog')

    params = settings.measure_smom
    S_win_len = params.S_win_len
    pre_window_start_sec = params.pre_window_start_sec
    max_S_P_time = params.max_S_P_time
    use_fixed_fmin_fmax = params.use_fixed_fmin_fmax
    fmin = params.fmin
    fmax = params.fmax
    phase_list = params.phase_list
    if not isinstance(phase_list, list):
        phase_list = [phase_list]

    if use_web_api:
        logger.info("Read from web_api")
        api_base_url = settings.seismic_api.base_url
        request = get_event_by_id(api_base_url, event_id)
        if request is None:
            logger.error("seismic api returned None!")
            exit(0)
        cat = request.get_event()
        st  = request.get_waveforms()

    else:
        logger.info("Read from files on disk")
        st = read(mseed_in, format='MSEED')
        # Fix station 4 channel code from 'Z' --> 'z':
        st.select(station='4')[0].stats.channel = 'z'

# Override web api cat with local input:
    if xml_in:
        cat  = read_events(xml_in)

    cat_out = cat.copy()

    inventory = app.get_inventory()
    missing_responses = st.attach_response(inventory)
    for sta in missing_responses:
        logger.warn("Inventory: Missing response for sta:%s" % sta)

# 1. Rotate traces to ENZ
    #st_rot = rotate_to_ENZ(st, inventory)
    #st = st_rot

# 2. Rotate traces to P,SV,SH wrt event location
    #st_new = rotate_to_P_SV_SH(st, cat_out)
    #st = st_new

    plot_fit = False

    for event in cat_out:
        origin = event.preferred_origin()
        synthetic_picks = app.synthetic_arrival_times(origin.loc, origin.time)

        for phase in phase_list:

            smom_dict, fc = measure_pick_smom(st, inventory, event,
                                              synthetic_picks,
                                              P_or_S=phase,
                                              fmin=fmin, fmax=fmax,
                                              use_fixed_fmin_fmax=use_fixed_fmin_fmax,
                                              plot_fit=plot_fit,
                                              debug_level=1,
                                              logger_in=logger)


            comment = Comment(text="corner_frequency_%s=%.2f measured for %s arrivals" % \
                              (phase, fc, phase))
            origin.comments.append(comment)

# Write out event xml for downstream modules (focal_mech, moment_mag)
    cat_out[0].write(xml_out, format='QUAKEML')

    return

if __name__ == '__main__':

    main()
