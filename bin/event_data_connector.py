import argparse
import logging
import os
from datetime import datetime
from time import sleep, time

import numpy as np
import pytz
from apscheduler.events import EVENT_ALL, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BlockingScheduler
from tenacity import (after_log, before_log, retry, stop_after_attempt,
                      wait_exponential)

from microquake.core import UTCDateTime
from microquake.IMS import web_client
from spp.utils import seismic_client
from spp.utils.application import Application

__module_name__ = "data_connector"
app = Application(module_name=__module_name__)
logger = app.logger


def get_and_post_IMS_data(
    api_base_url,
    ims_base_url,
    site,
    site_ids,
    tz,
    filter_existing_events=False,
):
    start_time, end_time = get_times(tz)
    IMS_events = retrieve_IMS_catalogue(
        ims_base_url,
        api_base_url,
        site,
        start_time,
        end_time,
        tz,
        filter_existing_events=filter_existing_events,
    )
    for event in IMS_events:
        post_event_to_api(
            event, ims_base_url, api_base_url, site_ids, site, tz
        )


def get_times(tz):
    end_time = UTCDateTime.now() - 3600
    start_time = end_time - 2 * 24 * 3600
    end_time = end_time.datetime.replace(tzinfo=pytz.utc).astimezone(tz=tz)
    start_time = start_time.datetime.replace(tzinfo=pytz.utc).astimezone(tz=tz)
    return start_time, end_time


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(7),
    after=after_log(logger, logging.DEBUG),
    before=before_log(logger, logging.DEBUG),
)
def retrieve_IMS_catalogue(
    ims_base_url,
    api_base_url,
    site,
    start_time,
    end_time,
    tz,
    filter_existing_events=False,
):
    logger.info("retrieving IMS catalogue (url:%s)", ims_base_url)
    ims_catalogue = web_client.get_catalogue(
        ims_base_url, start_time, end_time, site, tz, blast=False
    )
    if filter_existing_events:
        return filter_events(api_base_url, ims_catalogue, start_time, end_time)
    logger.info("retrieved IMS catalogue (url:%s)", ims_base_url)
    return ims_catalogue


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(7),
    after=after_log(logger, logging.DEBUG),
)
def filter_events(api_base_url, IMS_catalogue, start_time, end_time):
    logger.info(
        "filtering IMS list of %s events" % len(IMS_catalogue)
    )
    # if events exist in our system, do not re-upload them
    api_catalogue = seismic_client.get_events_catalog(
        api_base_url, start_time, end_time
    )
    api_existing_event_ids = {}
    for api_event in api_catalogue:
        api_existing_event_ids[api_event.event_resource_id] = True
    events_to_upload = []

    for IMS_event in IMS_catalogue:
        ims_resource_id = "{}/{}".format(IMS_event.resource_id.prefix, IMS_event.resource_id.id)
        if ims_resource_id not in api_existing_event_ids:
            events_to_upload.append(IMS_event)
    logger.info(
        "filtered IMS list of %s events down to %s events" % (len(IMS_catalogue), len(events_to_upload))
    )
    return events_to_upload


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(7),
    after=after_log(logger, logging.DEBUG),
)
def get_and_post_continuous_data(
    ims_base_url,
    api_base_url,
    event_time,
    resource_id,
    site_ids,
    tz,
    trim_wf=False,
):
    logger.info(
        "retrieving c_wf continuous data from IMS (url:%s)", ims_base_url
    )
    c_wf = web_client.get_continuous(
        ims_base_url, event_time - 1, event_time + 1, site_ids, tz
    )

    logger.info(
        "uploading continuous data to the SPP API (url:%s)", api_base_url
    )
    t0 = time()
    seismic_client.post_continuous_stream(
        api_base_url, c_wf, post_to_kafka=True, stream_id=resource_id
    )
    t1 = time()
    logger.info(
        "done uploading continuous stream to the SPP API in %0.3f seconds",
        (t1 - t0),
    )

    return c_wf


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(7),
    after=after_log(logger, logging.DEBUG),
)
def get_and_post_event_data(
    ims_base_url, api_base_url, event, event_time, tz, site_ids, context=None
):
    logger.info("retrieving vs_waveform from IMS (url:%s)", ims_base_url)
    vs_waveform = web_client.get_seismogram_event(
        ims_base_url, event, "OT", tz
    )
    cont_wf = web_client.get_continuous(
        ims_base_url, event_time - 10, event_time + 10, site_ids, tz
    )

    wf = clean_wf(cont_wf, event)

    logger.info("uploading data to the SPP API (url:%s)", api_base_url)
    logger.info(
        "objects types: event: %s, stream: %s, context: %s, vl_stream: %s",
        (type(event), type(vs_waveform), type(context), type(vs_waveform)),
    )

    t0 = time()
    seismic_client.post_data_from_objects(
        api_base_url,
        event_id=None,
        event=event,
        stream=wf,
        context_stream=context,
        variable_length_stream=vs_waveform,
        send_to_bus=True,
    )
    t1 = time()
    logger.info(
        "done uploading data to the SPP API in %0.3f seconds", (t1 - t0)
    )

    return vs_waveform


def clean_wf(stream, event):
    logger.info("Cleaning waveform with %s events", len(stream))

    wf = stream.copy()
    # Do some basic data quality tasks
    inventory = app.get_inventory()
    stations = inventory.stations()
    stations_code_ids = [station.code for station in stations]

    for trace in wf:
        if trace.stats.station not in stations_code_ids:
            wf.remove(trace)
        trace.stats.network = inventory.networks[0].code
        trace.stats.channel = trace.stats.channel.lower()

    pick_time = [
        arrival.get_pick().time
        for arrival in event.preferred_origin().arrivals
    ]
    min_pick_time = np.min(pick_time)

    for trace in wf:
        trace.data = np.nan_to_num(trace.data)

    wf = wf.detrend("demean").detrend("linear")

    wf = wf.taper(max_percentage=0.01, type="cosine", max_length=0.001)

    wf = wf.trim(
        starttime=min_pick_time - 0.5,
        endtime=min_pick_time + 1.5,
        pad=True,
        fill_value=0,
    )

    logger.info("Cleaned waveform with %s events", len(stream))

    return wf


def get_context(c_wf, arrivals):
    index = np.argmin([arrival.get_pick().time for arrival in arrivals])
    station_code = arrivals[index].get_pick().waveform_id.station_code

    context = (
        c_wf.select(station=station_code)
        .filter("bandpass", freqmin=60, freqmax=1000)
        .composite()
    )
    return context


def post_event_to_api(event, ims_base_url, api_base_url, site_ids, site, tz):
    event = web_client.get_picks_event(ims_base_url, event, site, tz)

    logger.info("extracting data for event %s", str(event))

    ts = []
    for arrival in event.preferred_origin().arrivals:
        ts.append(arrival.get_pick().time)
    event_time = np.min(ts)

    c_wf = get_and_post_continuous_data(
        ims_base_url, api_base_url, event_time, event.resource_id, site_ids, tz
    )
    context = get_context(c_wf, event.preferred_origin().arrivals)
    get_and_post_event_data(
        ims_base_url,
        api_base_url,
        event,
        event_time,
        tz,
        site_ids,
        context=context,
    )


def process_args():
    parser = argparse.ArgumentParser(
        description="Collect data from IMS and send them to the SPP-API"
    )
    parser.add_argument(
        "--mode",
        choices=["single", "cont"],
        default="single",
        help="the mode to run this module in. Options are single, cont (for continuously running this module)",
    )
    parser.add_argument("--filter_existing_events", default=True, type=bool)
    parser.add_argument(
        "--interval",
        default=1200,
        type=int,
        help="the interval in seconds to run this script",
    )

    return parser.parse_args()


def main():
    args = process_args()
    filter_existing_events = args.filter_existing_events
    mode = args.mode

    interval = args.interval

    site = app.get_stations()
    ims_base_url = app.settings.data_connector.path
    api_base_url = app.settings.seismic_api.base_url
    tz = app.get_time_zone()

    site_ids = [
        int(station.code)
        for station in site.stations()
        if station.code not in app.settings.sensors.black_list
    ]
    if mode == "single":
        logger.info("Retrieving and posting IMS data once")
        get_and_post_IMS_data(
            api_base_url,
            ims_base_url,
            site,
            site_ids,
            tz,
            filter_existing_events,
        )
    elif mode == "cont":
        logger.info("Retrieving and posting IMS data continuously")
        scheduler = BlockingScheduler()
        scheduler.add_executor("processpool")
        scheduler.add_job(
            get_and_post_IMS_data,
            "interval",
            seconds=interval,
            next_run_time=datetime.now(),
            max_instances=15,
            args=[
                api_base_url,
                ims_base_url,
                site,
                site_ids,
                tz,
                filter_existing_events,
            ],
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            app.logger.info("Exiting")


if __name__ == "__main__":
    main()
