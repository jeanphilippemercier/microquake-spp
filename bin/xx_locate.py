
from microquake.core import read
from microquake.core import UTCDateTime
from microquake.core.data.inventory import inv_station_list_to_dict
from microquake.core.event import read_events as read_events
from microquake.nlloc import NLL, calculate_uncertainty
from spp.utils.application import Application
from spp.utils.seismic_client import RequestEvent, get_events_catalog, get_event_by_id

from lib_process import fix_arr_takeoff_and_azimuth, processCmdLine

fname = 'locate'

def main():

    use_web_api, event_id, xml_out, xml_in, mseed_in = processCmdLine(fname, require_mseed=False)

    # reading application data
    app = Application()
    settings = app.settings
    logger = app.get_logger('xx_locate', 'zlog')

    if use_web_api:
        logger.info("Read from web_api")
        api_base_url = settings.seismic_api.base_url
        request = get_event_by_id(api_base_url, event_id)
        if request is None:
            logger.error("seismic api returned None!")
            exit(0)
        cat = request.get_event()

    else:
        logger.info("Read from files on disk")
        cat  = read_events(xml_in)

    #snr_picks = [ pk for pk in cat_out[0].picks if pk.method is not None and 'snr_picker' in pk.method ]

    location = __import__('04_hypocenter_location').process
    params = app.settings.nlloc
    logger.info('Preparing NonLinLoc')

    project_code = settings.project_code
    base_folder = settings.nlloc.nll_base
    gridpar = app.nll_velgrids()
    sensors = app.nll_sensors()

    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar, sensors=sensors, params=params)
    prepared_objects = {"nll": nll}

    # This will create a new (3rd) origin and will set cat_out[0].preferred_origin to point to it,
    #   however, event will still contain only 2 origins and event.preferred_origin points to the old preferred

    cat_out, st_out = location(cat=cat, stream=None,
                               app=app,
                               module_settings=app.settings.nlloc,
                               prepared_objects=prepared_objects,
                               logger=logger)

    cat_out.write(xml_out, format='QUAKEML')

    return

if __name__ == '__main__':

    main()