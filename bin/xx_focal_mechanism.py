
import numpy as np
from obspy.core.event.base import ResourceIdentifier
from microquake.core.event import read_events
from microquake.focmec.core import calc_focal_mechanisms
from spp.utils.application import Application
from spp.utils.seismic_client import get_event_by_id

from lib_process import processCmdLine


def main():

    fname = 'focal_mechanism'

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger('xx_focal_mechanism', 'zlog')

    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname, require_mseed=False)

    if use_web_api:
        api_base_url = settings.seismic_api.base_url
        request = get_event_by_id(api_base_url, event_id)
        if request is None:
            logger.error("seismic api returned None!")
            exit(0)
        cat = request.get_event()

    else:
        cat  = read_events(xml_in)


    focal_mechanisms, figs = calc_focal_mechanisms(cat, settings.focal_mechanism, logger_in=logger)

    for i,event in enumerate(cat):
        focal_mechanism = focal_mechanisms[i]
        event.focal_mechanisms = [ focal_mechanism ]
        event.preferred_focal_mechanism_id = ResourceIdentifier(id=focal_mechanism.resource_id.id, \
                                                                referred_object=focal_mechanism)

    for i,fig in enumerate(figs):
        fig.savefig('foc_mech_%d.png' % i)


    cat.write(xml_out, format='QUAKEML')

    return


if __name__ == '__main__':
    main()
