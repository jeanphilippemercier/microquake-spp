import argparse
import logging
from datetime import datetime

import numpy as np
from apscheduler.schedulers.background import BlockingScheduler
from tenacity import after_log, retry, stop_after_attempt, wait_exponential

from bin.analyse_signal import __module_name__ as analyse_signal_module_name
from bin.analyse_signal import process as analyse_signal
from microquake.core import UTCDateTime
from microquake.IMS import web_client
from spp.utils import seismic_client
from spp.utils.application import Application
from spp.utils.seismic_client import get_station2, post_station2

__module_name__ = "continuous_data_connector"
app = Application(module_name=__module_name__)
logger = app.logger

DEFAULT_WINDOW_SIZE = 30
DEFAULT_END_TIME = UTCDateTime.now() - (3600 * 2)
DEFAULT_START_TIME = DEFAULT_END_TIME - DEFAULT_WINDOW_SIZE


def process_continuous_data(
    app,
    logger,
    ims_base_url,
    api_base_url,
    start_time,
    end_time,
    window_size,
    site_ids,
    tz,
):
    if end_time is None or start_time is None:
        end_time = UTCDateTime.now() - (3600 * 2)
        start_time = end_time - window_size
    wf = get_continuous_data(
        app, logger, ims_base_url, start_time, end_time, site_ids, tz
    )
    signal_quality_data = process_signal_analysis(app, logger, wf)
    post_all_stations_signals_analysis(
        logger, api_base_url, signal_quality_data
    )


def get_continuous_data(
    app, logger, ims_base_url, start_time, end_time, site_ids, tz
):
    logger.info(
        "Retrieving continuous data from IMS. Start time %s end time %s"
        % (str(start_time), str(end_time))
    )
    wf = web_client.get_continuous(
        ims_base_url, start_time, end_time, site_ids, tz
    )

    logger.info("Retrieved continuous data from IMS")
    return wf


def process_signal_analysis(app, logger, stream):
    logger.info("Analysing signal quality. Stream length %s" % len(stream))
    signal_quality_data = analyse_signal(
        cat=None,
        stream=stream,
        logger=logger,
        app=app,
        prepared_objects=None,
        module_settings=app.settings[analyse_signal_module_name],
    )
    logger.info("Analysed signal quality")
    return signal_quality_data


def post_all_stations_signals_analysis(
    logger, api_base_url, signal_quality_data
):
    logger.info("Posting all station signal quality data")
    api_base_url = app.settings.seismic_api.base_url
    for station in signal_quality_data:
        logger.info(
            "posting data quality information to the API for station: %s"
            % station["station_code"]
        )
        post_station_signals_analysis(
            api_base_url,
            station["station_code"],
            station["energy"],
            station["integrity"],
        )
        logger.info("posted station data")
    logger.info("Done posting all station signal quality data")


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(7),
    after=after_log(logger, logging.DEBUG),
)
def post_station_signals_analysis(
    api_base_url, station_code, energy, integrity
):
    station = get_station2(api_base_url, station_code)
    if station is None:
        station = {
            "name": "OT-station-{}".format(station_code),
            "code": station_code,
        }

    station["energy"] = np.nan_to_num(energy)
    station["integrity"] = np.nan_to_num(integrity)
    result = post_station2(api_base_url, station)
    if result.status_code != 201:
        raise Exception(
            "Could not post signals analysis data to api, status code {}".format(
                result.status_code
            )
        )


def process_args():
    parser = argparse.ArgumentParser(
        description="Collect continuous data from IMS, analyse the signal and send it to the SPP-API"
    )
    parser.add_argument(
        "--mode",
        choices=["single", "cont"],
        default="single",
        help="the mode to run this module in. Options are single, cont (for continuously running this module)",
    )
    parser.add_argument(
        "--window_size",
        default=DEFAULT_WINDOW_SIZE,
        type=int,
        help="the number of seconds of data to process at a time",
    )
    parser.add_argument(
        "--start_time",
        default=DEFAULT_START_TIME,
        type=lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S"),
        help="the start time for data to process in YYYY-MM-DDTH:M:S eg: 2019-03-18T09:01:21",
    )
    parser.add_argument(
        "--end_time",
        default=DEFAULT_END_TIME,
        type=lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S"),
        help="the end time for data to process in YYYY-MM-DDTH:M:S eg: 2019-03-18T09:01:21",
    )

    return parser.parse_args()


def main():
    args = process_args()
    mode = args.mode
    window_size = args.window_size
    end_time = args.end_time
    start_time = args.start_time

    site = app.get_stations()
    ims_base_url = app.settings.data_connector.path
    api_base_url = app.settings.seismic_api.base_url
    tz = app.get_time_zone()

    site_ids = [
        int(station.code)
        for station in site.stations()
        if station.code not in app.settings.sensors.black_list
    ]

    app.logger.info(
        "Retrieving continuous data, analysing signals, and posting to API"
    )
    if mode == "single":
        app.logger.info("Processing a single block of data")
        process_continuous_data(
            app,
            app.logger,
            ims_base_url,
            api_base_url,
            start_time,
            end_time,
            None,
            site_ids,
            tz,
        )
    elif mode == "cont":
        app.logger.info("Continuously running and processing all data")
        scheduler = BlockingScheduler()
        scheduler.add_executor("processpool")
        scheduler.add_job(
            process_continuous_data,
            "interval",
            seconds=window_size,
            next_run_time=datetime.now(),
            max_instances=10,
            args=[
                app,
                app.logger,
                ims_base_url,
                api_base_url,
                None,
                None,
                window_size,
                site_ids,
                tz,
            ],
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            app.logger.info("Exiting")


if __name__ == "__main__":
    main()
