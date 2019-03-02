
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

    fname = 'measure_amplitudes'
    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname)

    # reading application data
    app = Application(module_name='nlloc')
    settings = app.settings

    logger = app.get_logger('xx_measure_amplitudes','zlog')

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
    st_rot = rotate_to_ENZ(st, inventory)
    st = st_rot
    # MTH: st_rot now contains a mix of rotated (P,SV,SH) and non-rotate(E,N,Z) traces
    # since if we have no arrival we have no backazimuth and hence no rotation!
    # However, measure_pick_amps is only going to measure traces that are associated
    # with an arrival

# 2. Rotate traces to P,SV,SH wrt event location
    st_new = rotate_to_P_SV_SH(st, cat_out)
    st = st_new

# 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces

    # MTH: This time-domain method only seems (to me) to be reliable for
    #      for measuring P polarity and displacement_area.
    #      For S I prefer to use freq domain approach in a separate module

    trP = [tr for tr in st if tr.stats.channel == 'P' or tr.stats.channel.upper() == 'Z']
    measure_pick_amps(Stream(traces=trP), cat_out, phase_list=['P'], use_stats_dict=False,
                      min_pulse_width=.00167, min_pulse_snr=5, debug=True)

# Write out new event xml file with arrival dicts containing the amp measurements
#   needed by moment_mag and focal_mech modules:
    cat_out[0].write(xml_out, format='QUAKEML')


    return

if __name__ == '__main__':

    main()
