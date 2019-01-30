
'''
import os
import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")
'''

from microquake.core import read
from microquake.core.event import read_events as read_events
from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.waveform.amp_measures import measure_pick_amps
from microquake.waveform.smom_mag import measure_pick_smom

from spp.utils.application import Application

from lib_process import *
from helpers import *


def main():

    fname = 'measure_amplitudes'

    # reading application data
    app = Application()
    settings = app.settings

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()

    logger = app.get_logger('test_xml','test_xml.log')

    data_dir   = '/Users/mth/mth/Data/OT_data/'
    mseed_file = data_dir + "20180706112101.mseed"
    mseed_file = data_dir + "20180628153305.mseed"
    mseed_file = data_dir + "20180609195044.mseed"
    mseed_file = data_dir + "20180523111608.mseed"
# MTH!!
    event_file = 'event.xml'
    st = read(mseed_file, format='MSEED')

    # Fix 4..Z channel name:
    for tr in st:
        tr.stats.channel = tr.stats.channel.lower()


    event  = read_events(event_file)[0]

    inventory = app.get_inventory()
    st.attach_response(inventory)

    sta_meta_dict = inv_station_list_to_dict(inventory)


# 1. Rotate traces to ENZ
    st_rot = rotate_to_ENZ(st, sta_meta_dict)
    st = st_rot

    noisy_channels = remove_noisy_traces(st, event.picks)
    for tr in noisy_channels:
        print("%s: Removed noisy tr:%s" % (fname, tr.get_id()))

    cat_out = [event]

# 2. Rotate traces to P,SV,SH wrt event location
    st_new = rotate_to_P_SV_SH(st, cat_out)
    st = st_new

# 3. Measure polarities, displacement areas, etc for each pick from instrument deconvolved traces
    trP = [tr for tr in st if tr.stats.channel == 'P']
    measure_pick_amps(Stream(traces=trP), cat_out, phase_list=['P'], debug=False)

    #trS = [tr for tr in st if tr.stats.channel[0] == 'S']
    #measure_pick_amps(Stream(traces=trS), cat_out, phase_list=['S'], debug=False)

#   It's much slower to calculate lambda in the freq domain but here it is:
    calc_smom = True
    calc_smom = False
    if calc_smom:
        origin = event.preferred_origin()
        synthetic_picks = app.synthetic_arrival_times(origin.loc, origin.time)
        smom_dict = measure_pick_smom(st, inventory, event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='P', debug=False)
        #smom_dict = measure_pick_smom(st, inventory, event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='S', debug=False)
        #measure_pick_smom(st, inventories[0], event, synthetic_picks, fmin=20., fmax=1e3, P_or_S='S', debug=False)


# Write out event xml for downstream modules (focal_mech, moment_mag)
    cat_out[0].write('event_1.xml', format='QUAKEML')

    return

if __name__ == '__main__':

    main()
