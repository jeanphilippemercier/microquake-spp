
import os
import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore")

from obspy.core.event.base import ResourceIdentifier

from microquake.core import read
from microquake.core.event import read_events as read_events

from spp.utils.application import Application

from lib_process import *
from helpers import *


def main():

    fname = 'calc_focal_mechanism'

    # reading application data
    app = Application()
    settings = app.settings

    cat = read_events('event_2.xml')

    focal_mechanisms = calc_focal_mechanism(cat, settings.focal_mechanism)
    for i,event in enumerate(cat):
        focal_mechanism = focal_mechanisms[i]
        event.focal_mechanisms = [ focal_mechanism ]
        event.preferred_focal_mechanism_id = ResourceIdentifier(id=focal_mechanism.resource_id.id, referred_object=focal_mechanism)

    cat.write("event_3.xml", format='QUAKEML')



def calc_focal_mechanism(cat, settings):

    sname  = []
    p_pol  = []
    p_qual = []
    qdist = []
    qazi = []
    qthe = []
    sazi = []
    sthe = []


    events = []

    for event in cat:

        event_dict = {}

        origin = event.preferred_origin()

        event_dict['event_info'] = origin.time.datetime.strftime('%Y-%m-%d %H:%M:%S')
        event_dict['event'] = {}
        event_dict['event']['qdep']= origin.loc[2]
        event_dict['event']['sez']= 10.
        event_dict['event']['icusp']= 1234567

        arrivals = [arr for arr in event.preferred_origin().arrivals if arr.phase == 'P']

        for arr in arrivals:

            if arr.pulse_snr is None:
                print("%s P arr pulse_snr == NONE !!!" % arr.pick_id.get_referred_object().waveform_id.station_code)
                continue

            sname.append(arr.pick_id.get_referred_object().waveform_id.station_code)
            p_pol.append(arr.polarity)
            qdist.append(arr.distance)
            qazi.append(arr.azimuth)
            #qthe.append(arr.takeoff_angle)
    # MTH: both HASH and test_stereo expect takeoff theta measured wrt vertical Up!
            qthe.append(180. - arr.takeoff_angle)
            sazi.append(2.)
            sthe.append(10.)

            if arr.pulse_snr >= 100.:
                qual = 0
            else:
                qual = 1
            p_qual.append(qual)

        event_dict['sname'] = sname
        event_dict['p_pol'] = p_pol
        event_dict['p_qual'] = p_qual
        event_dict['qdist'] = qdist
        event_dict['qazi'] = qazi
        event_dict['qthe'] = qthe
        event_dict['sazi'] = sazi
        event_dict['sthe'] = sthe

    events.append(event_dict)

    """
    for azi in sorted(events[0]['qazi']):
        print(azi)
    exit()
    """

    from hashpy.scripts.mth_new import calc_focal_mechanisms
    outputs = calc_focal_mechanisms(events, settings, phase_format='FPFIT')
    from obspy.core.event.source import FocalMechanism, NodalPlane, NodalPlanes
    from obspy.imaging.beachball import aux_plane
    from obspy.core.event.base import Comment

    focal_mechanisms = []

    for i,out in enumerate(outputs):
        print("=== %d: Process Focal Mech" % i)
        p1 = NodalPlane(strike=out['strike'], dip=out['dip'], rake=out['rake'])
        s,d,r = aux_plane(out['strike'], out['dip'], out['rake'])
        p2 = NodalPlane(strike=s, dip=d, rake=r)

        fc = FocalMechanism(nodal_planes = NodalPlanes(nodal_plane_1=p1, nodal_plane_2=p2),
                            azimuthal_gap = out['azim_gap'],
                            station_polarity_count = out['station_polarity_count'],
                            station_distribution_ratio = out['stdr'],
                            misfit = out['misfit'],
                            evaluation_mode = 'automatic',
                            evaluation_status = 'preliminary',
                            comments = [Comment(text="HASH v1.2 Quality=[%s]" % out['quality'])]
                           )
        print(fc)
        focal_mechanisms.append(fc)

        event = events[i]

        title = "%s (s,d,r)_1=(%.1f,%.1f,%.1f) _2=(%.1f,%.1f,%.1f)" % \
                (event['event_info'], p1.strike, p1.dip, p1.rake, p2.strike,p2.dip,p2.rake)

        test_stereo(np.array(event['qazi']),np.array(event['qthe']),np.array(event['p_pol']),\
                    sdr=[p1.strike,p1.dip,p1.rake], title=title)


    return focal_mechanisms

if __name__ == '__main__':

    main()
