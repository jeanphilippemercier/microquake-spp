
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
    app = Application()
    settings = app.settings

    logger = app.get_logger('test_xml','test_xml.log')

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

# Override web api cat with local input:
    if xml_in:
        #print("Override cat with xml_in!")
        cat  = read_events(xml_in)

    cat_out = cat.copy()

    inventory = app.get_inventory()
    st.attach_response(inventory)

# TODO: pass inventory straight into transform.rotations ?

    sta_meta_dict = inv_station_list_to_dict(inventory)

# 1. Rotate traces to ENZ
    st_rot = rotate_to_ENZ(st, sta_meta_dict)
    st = st_rot

# 2. Rotate traces to P,SV,SH wrt event location
    st_new = rotate_to_P_SV_SH(st, cat_out)
    st = st_new

# 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
    trP = [tr for tr in st if tr.stats.channel == 'P']
    measure_pick_amps(Stream(traces=trP), cat_out, phase_list=['P'], \
                      min_pulse_width=.00167, min_pulse_snr=5, debug=False)
    # TODO: eventually will probably want to move min_pulse params to toml file

    '''

# Write out new event xml file with arrival dicts containing the amp measurements
#   needed by moment_mag and focal_mech modules:
    cat_out[0].write(xml_out, format='QUAKEML')

    exit()

  # Zach: I'm still working out the flow for the S arrivals below here, so ignore for now ..
    '''

    trS = [tr for tr in st_rot if tr.stats.channel[0].upper() != 'Z']
    st2 = Stream(traces=trS)
    #measure_pick_amps(Stream(traces=trS), cat_out, phase_list=['S'], debug=False, use_stats_dict=True)
    measure_pick_amps(st2, cat_out, phase_list=['S'], debug=False, use_stats_dict=True)

    arrivals = [arr for arr in event.preferred_origin().arrivals if arr.phase == 'S']
    arrival_dict = {}
    for arr in arrivals:
        pk = arr.pick_id.get_referred_object()
        sta = pk.waveform_id.station_code
        pha = arr.phase
        if pha != 'S':
            print("Phase != S !!")
            exit()
        arrival_dict[sta] = arr

    stations = st2.unique_stations()
    for sta in stations:
        trs = st2.select(station=sta)
        if len(trs) == 1:
            print("sta:%s ch1:%s No other channels!" % (sta, trs[0].stats.channel))
        else:
            print("sta:%s ch1:%s ch2:%s" % (sta, trs[0].stats.channel, trs[1].stats.channel))
            if 'S_arrival' in trs[0].stats and 'S_arrival' in trs[1].stats:
                if trs[0].stats.S_arrival.velocity_pulse.polarity != 0 and \
                   trs[1].stats.S_arrival.velocity_pulse.polarity != 0:
                    print(trs[0].stats.S_arrival.velocity_pulse.polarity)
                    print(trs[1].stats.S_arrival.velocity_pulse.polarity)
                    print(trs[0].stats.S_arrival.displacement_pulse.dis_pulse_area)
                    print(trs[1].stats.S_arrival.displacement_pulse.dis_pulse_area)

                    A_E = trs[0].stats.S_arrival.displacement_pulse.dis_pulse_area
                    A_N = trs[1].stats.S_arrival.displacement_pulse.dis_pulse_area
                    S_dis_area = 0.5 * (np.abs(A_E) + np.abs(A_N))

                    arrival_dict[sta].dis_pulse_area = S_dis_area

#   It's much slower to calculate lambda in the freq domain but here it is:
    calc_smom = False
    calc_smom = True
    if calc_smom:
        origin = event.preferred_origin()
        synthetic_picks = app.synthetic_arrival_times(origin.loc, origin.time)
        smom_dict = measure_pick_smom(st, inventory, event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='P', debug=False)
        smom_dict = measure_pick_smom(st, inventory, event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='S', debug=False)
        #measure_pick_smom(st, inventories[0], event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='S', debug=False)


# Write out event xml for downstream modules (focal_mech, moment_mag)
    cat_out[0].write('event_1.xml', format='QUAKEML')

    return

if __name__ == '__main__':

    main()
