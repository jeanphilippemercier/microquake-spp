
from obspy.core.event.base import ResourceIdentifier

from microquake.core import read
from microquake.core.event import read_events as read_events
from spp.utils.application import Application
from spp.utils.seismic_client import RequestEvent, get_events_catalog, get_event_by_id

from lib_process import fix_arr_takeoff_and_azimuth, processCmdLine

fname = 'snr_picker'

def main():

    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname)

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger('xx_picker', 'zlog')

    if use_web_api:
        api_base_url = settings.seismic_api.base_url
        request = get_event_by_id(api_base_url, event_id)
        if request is None:
            logger.error("seismic api returned None!")
            exit(0)
        cat = request.get_event()
        st  = request.get_waveforms()

    else:
        st = read(mseed_in, format='MSEED')

        # Fix broken preferred:
        cat  = read_events(xml_in)
        event  = cat[0]
        origin = event.origins[0]
        event.preferred_origin_id = ResourceIdentifier(id=origin.resource_id.id, referred_object=origin)
        mag = event.magnitudes[0]
        event.preferred_magnitude_id = ResourceIdentifier(id=mag.resource_id.id, referred_object=mag)

    picker = __import__('03_picker').process
    params = app.settings.picker
    # This will create a new (2nd) origin with origin.time from stacking and origin.loc same as original orogin.loc
    #  The new origin will have arrivals for each snr pick that exceeded snr_threshold
    # Both event.preferred_origin and cat_out[0].preferred_origin will be set to this new (2nd) origin
    #cat_out, st_out = picker(cat=cat, stream=st, extra_msgs=None, logger=logger, params=params, app=app)
    cat_out, st_out = picker(cat=cat, stream=st, app=app, module_settings = app.settings.picker, logger=logger)

    cat_out.write(xml_out, format='QUAKEML')

    return

if __name__ == '__main__':

    main()
