import argparse
from time import time

import numpy as np
import pytz
from tenacity import retry, wait_exponential

from microquake.core import UTCDateTime
from microquake.IMS import web_client
from spp.utils import seismic_client
from spp.utils.application import Application


def continuously_send_IMS_data(api_base_url, ims_base_url, logger, site, site_ids, tz, filter_existing_events=False, post_continuous_data=False, trim_wf=False):
    try:
        while True:
            get_and_post_IMS_data(api_base_url, ims_base_url, logger, site, site_ids, tz, filter_existing_events=filter_existing_events, post_continuous_data=post_continuous_data, trim_wf=trim_wf)
    except KeyboardInterrupt:
        logger.info("received keyboard interrupt")


def get_and_post_IMS_data(api_base_url, ims_base_url, logger, site, site_ids, tz, filter_existing_events=False, post_continuous_data=False, trim_wf=False):
    start_time, end_time = get_times(tz)
    IMS_events = retrieve_IMS_catalogue(ims_base_url, api_base_url, site, start_time, end_time, tz, filter_existing_events=filter_existing_events)
    events_to_upload = filter_existing_events(IMS_events)
    for event in events_to_upload:
        post_event_to_api(event, ims_base_url, api_base_url, site_ids, logger, site, tz, post_continuous_data=post_continuous_data, trim_wf=trim_wf)


def get_times(tz):
    end_time = UTCDateTime.now() - 7200
    start_time = end_time - 5 * 24 * 3600  # 4 days
    end_time = end_time.datetime.replace(tzinfo=pytz.utc).astimezone(tz=tz)
    start_time = start_time.datetime.replace(tzinfo=pytz.utc).astimezone(tz=tz)
    return start_time, end_time

@retry(wait=wait_exponential(multiplier=1, min=1, max=10))
def retrieve_IMS_catalogue(ims_base_url, api_base_url, site, start_time, end_time, tz, filter_existing_events=False):
    ims_catalogue = web_client.get_catalogue(ims_base_url, start_time, end_time,
                                    site, tz, blast=False)
    if filter_existing_events:
        return filter_events(api_base_url, ims_catalogue, start_time, end_time)
    return ims_catalogue

@retry(wait=wait_exponential(multiplier=1, min=1, max=10))
def filter_events(api_base_url, IMS_catalogue, start_time, end_time):
    # if events exist in our system, do not re-upload them
    api_catalogue = seismic_client.get_events_catalog(api_base_url, start_time, end_time)
    api_existing_event_ids = {}
    for api_event in api_catalogue:
        api_existing_event_ids[api_event.resource_id] = True
    events_to_upload = []
    for IMS_event in IMS_catalogue:
        if IMS_event.resource_id not in api_existing_event_ids:
            events_to_upload.append(IMS_event)
    return events_to_upload

@retry(wait=wait_exponential(multiplier=1, min=1, max=10))
def get_and_post_continuous_data(ims_base_url, api_base_url, event_time, logger, resource_id, site_ids, tz, trim_wf=False):
    logger.info('retrieving c_wf continuous data from IMS (url:%s)' % ims_base_url)
    c_wf = web_client.get_continuous(ims_base_url, event_time - 1, event_time + 1, site_ids, tz)
    if trim_wf:
        c_wf.trim(starttime=event_time - 1., endtime=event_time + 1.)

    logger.info('uploading continuous data to the SPP API (url:%s)' % api_base_url)
    t0 = time()
    seismic_client.post_continuous_stream(api_base_url, c_wf,
                                          post_to_kafka=True,
                                          stream_id=resource_id)
    t1 = time()
    logger.info('done uploading continuous stream to the SPP API in %0.3f seconds' % (t1 - t0))

    return c_wf


@retry(wait=wait_exponential(multiplier=1, min=1, max=10))
def get_and_post_event_data(ims_base_url, api_base_url, event, event_time, logger, tz, context=None):
    logger.info('retrieving vs_waveform from IMS (url:%s)' % ims_base_url)
    vs_waveform = web_client.get_seismogram_event(ims_base_url, event, 'OT', tz)
    wf = vs_waveform.copy().trim(starttime=event_time - 1., endtime=event_time + 1.)

    logger.info('uploading data to the SPP API (url:%s)' % api_base_url)
    logger.info('objects types: event: %s, stream: %s, context: %s, vl_stream: %s' % (type(event), type(vs_waveform), type(context), type(vs_waveform)))

    t0 = time()
    seismic_client.post_data_from_objects(api_base_url, event_id=None,
                                          event=event, stream=wf,
                                          context_stream=context,
                                          variable_length_stream=vs_waveform,
                                          logger=logger)
    t1 = time()
    logger.info('done uploading data to the SPP API in %0.3f seconds' % (t1 - t0))

    return vs_waveform


def get_context(c_wf, arrivals, context):
    index = np.argmin([arrival.distance for arrival in arrivals])

    station_code = arrivals[index].get_pick().waveform_id.station_code

    context = c_wf.select(station=station_code).filter('bandpass',
                                                        freqmin=60,
                                                        freqmax=1000).composite()
    return context


def post_event_to_api(event, ims_base_url, api_base_url, site_ids, logger, site, tz, post_continuous_data=False, trim_wf=False):
    event = web_client.get_picks_event(ims_base_url, event, site, tz)

    logger.info('extracting data for event %s' % str(event))
    event_time = event.preferred_origin().time

    context = None
    if post_continuous_data:
        c_wf = get_and_post_continuous_data(ims_base_url, api_base_url, event_time, logger, event.resource_id, site_ids, tz, trim_wf=trim_wf)
        context = get_context(c_wf, event.preferred_origin().arrivals, context)
        get_and_post_event_data(ims_base_url, api_base_url, event, event_time, logger, tz, context=context)
    else:
        get_and_post_event_data(ims_base_url, api_base_url, event, event_time, logger, tz, context=None)


def process_args():
    parser = argparse.ArgumentParser(description="Collect data from IMS and send them to the SPP-API")
    parser.add_argument(
        "--mode",
        choices=["single", "cont"],
        default="single",
        help="the mode to run this module in. Options are single, cont (for continuously running this module)",
    )
    parser.add_argument("--filter_existing_events", default=True, type=bool)
    return parser.parse_args()


__module_name__ = "data_connector"


def main():
    args = process_args()
    filter_existing_events = args.filter_existing_events
    mode = args.mode

    app = Application(__module_name__)
    site = app.get_stations()
    ims_base_url = app.settings.data_connector.path
    api_base_url = app.settings.seismic_api.base_url
    tz = app.get_time_zone()

    site_ids = [int(station.code) for station in site.stations() if station.code not in app.settings.sensors.black_list]

    if mode == 'single':
        get_and_post_IMS_data(api_base_url, ims_base_url, app.logger, site, site_ids, tz, filter_existing_events)
    elif mode == 'cont':
        continuously_send_IMS_data(api_base_url, ims_base_url, app.logger, site, site_ids, tz, filter_existing_events)


if __name__ == "__main__":
    main()
